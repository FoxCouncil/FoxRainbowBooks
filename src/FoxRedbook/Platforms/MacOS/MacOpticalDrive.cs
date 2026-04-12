using System.Buffers;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using FoxRedbook.Platforms.Common;

namespace FoxRedbook.Platforms.MacOS;

/// <summary>
/// macOS implementation of <see cref="IOpticalDrive"/> using IOKit's
/// SCSITaskDeviceInterface for SCSI passthrough. Opens an optical drive
/// identified by its BSD name (e.g., <c>disk1</c> or <c>/dev/disk1</c>),
/// unmounts any auto-mounted filesystem view of the disc via DiskArbitration,
/// claims exclusive access, issues INQUIRY / READ TOC / READ CD commands,
/// and remounts the disc on Dispose.
/// </summary>
/// <remarks>
/// <para>
/// The CDB builders, response parsers, and sense-data mapping all come
/// from <see cref="ScsiCommands"/>, shared with the Linux and Windows
/// backends. Only the IOKit plumbing and DiskArbitration lifecycle are
/// macOS-specific.
/// </para>
/// <para>
/// <b>COM-style vtable navigation</b>: the macOS SCSI passthrough API is a
/// COM plug-in loaded via CFPlugIn. An "interface pointer" in this API is
/// actually a pointer to a pointer to a vtable — <c>iface**</c>. To call a
/// method, the C convention is <c>(*iface)-&gt;Method(iface, args)</c>:
/// <list type="number">
///   <item>Dereference once to get the vtable struct</item>
///   <item>Read the function pointer out of the struct at the right offset</item>
///   <item>Cast the function pointer to a <c>delegate* unmanaged</c> matching the method signature</item>
///   <item>Invoke it with the outer pointer (iface, not *iface) as the first argument</item>
/// </list>
/// This is NOT the same as IOKit's <c>IOObjectRelease</c> (which operates on
/// mach ports) or CoreFoundation's <c>CFRelease</c> (which operates on
/// CFTypeRef). COM-style interfaces have their own reference counting via
/// the <c>Release</c> function pointer on their vtable. Mixing them up
/// corrupts reference counts and either leaks kernel resources or crashes
/// when the runtime double-frees an object.
/// </para>
/// </remarks>
[SupportedOSPlatform("macos")]
public sealed class MacOpticalDrive : IOpticalDrive
{
    private const uint DefaultTimeoutMs = 30_000;
    private const int SenseBufferSize = 32;
    private const double UnmountTimeoutSeconds = 30.0;

    private readonly string _bsdName;
    private readonly SafeCFTypeRefHandle _daSession;
    private readonly SafeCFTypeRefHandle _daDisk;
    private readonly SafeIoObjectHandle _service;

    // COM interface pointers (NOT SafeHandles — managed manually via the
    // vtable's Release method, see the class remarks).
    private IntPtr _scsiDeviceInterfacePtr;  // SCSITaskDeviceInterface**
    private bool _exclusiveAccessHeld;

    private DriveInquiry? _cachedInquiry;
    private bool _disposed;

    /// <summary>
    /// Opens the given BSD device and returns an <see cref="IOpticalDrive"/>
    /// backed by IOKit SCSI passthrough.
    /// </summary>
    /// <param name="devicePath">
    /// BSD device name — <c>disk1</c>, <c>/dev/disk1</c>, etc. The <c>/dev/</c>
    /// prefix is stripped internally before calling <c>IOBSDNameMatching</c>.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="devicePath"/> is null.</exception>
    /// <exception cref="ArgumentException">Path is not a recognized form.</exception>
    /// <exception cref="OpticalDriveException">
    /// IOKit or DiskArbitration operation failed — the specific failure is
    /// included in the message.
    /// </exception>
    public MacOpticalDrive(string devicePath)
    {
        ArgumentNullException.ThrowIfNull(devicePath);

        _bsdName = MacBsdName.Normalize(devicePath);

        // Phase 1: DiskArbitration — create session, create disk, unmount the
        // auto-mounted filesystem view (if any). Without this, ObtainExclusiveAccess
        // below fails with kIOReturnBusy.
        IntPtr sessionPtr = DiskArbitrationNative.DASessionCreate(IntPtr.Zero);

        if (sessionPtr == IntPtr.Zero)
        {
            throw new OpticalDriveException("DASessionCreate returned NULL.");
        }

        _daSession = new SafeCFTypeRefHandle(sessionPtr);

        IntPtr runLoop = DiskArbitrationNative.CFRunLoopGetCurrent();
        IntPtr defaultMode = DiskArbitrationNative.GetCFRunLoopDefaultMode();

        DiskArbitrationNative.DASessionScheduleWithRunLoop(sessionPtr, runLoop, defaultMode);

        try
        {
            IntPtr diskPtr = DiskArbitrationNative.DADiskCreateFromBSDName(
                IntPtr.Zero, sessionPtr, _bsdName);

            if (diskPtr == IntPtr.Zero)
            {
                throw new OpticalDriveException(
                    $"DADiskCreateFromBSDName failed for '{_bsdName}' — no matching disk found.");
            }

            _daDisk = new SafeCFTypeRefHandle(diskPtr);

            // Unmount the virtual filesystem view. Non-destructive: only the
            // filesystem layer detaches; the disc stays in the drive.
            UnmountDisk(diskPtr, remount: false);

            // Phase 2: IOKit service discovery from the BSD name.
            IntPtr servicePtr = FindIoService(_bsdName);
            _service = new SafeIoObjectHandle(servicePtr);

            // Phase 3: Load the SCSI plug-in interface and claim exclusive access.
            _scsiDeviceInterfacePtr = LoadScsiInterface(servicePtr);

            ObtainExclusiveAccess(_scsiDeviceInterfacePtr);
            _exclusiveAccessHeld = true;
        }
        catch
        {
            // Best-effort cleanup of anything we've already acquired.
            ReleaseScsiInterface();
            _service?.Dispose();

            if (_daDisk is not null && !_daDisk.IsInvalid)
            {
                TryRemount();
            }

            _daDisk?.Dispose();
            DiskArbitrationNative.DASessionUnscheduleFromRunLoop(sessionPtr, runLoop, defaultMode);
            _daSession.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public DriveInquiry Inquiry
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _cachedInquiry ??= QueryInquiry();
        }
    }

    /// <inheritdoc />
    public Task<TableOfContents> ReadTocAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        byte[] cdb = new byte[10];
        ScsiCommands.BuildReadToc(cdb);

        byte[] response = ArrayPool<byte>.Shared.Rent(ScsiCommands.ReadTocMaxAllocationLength);

        try
        {
            ExecuteScsiCommand(cdb, response.AsSpan(0, ScsiCommands.ReadTocMaxAllocationLength));
            TableOfContents toc = ScsiCommands.ParseReadTocResponse(
                response.AsSpan(0, ScsiCommands.ReadTocMaxAllocationLength));
            return Task.FromResult(toc);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(response);
        }
    }

    /// <inheritdoc />
    public Task<CdText?> ReadCdTextAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        byte[] cdb = new byte[10];
        CdTextCommands.BuildReadCdText(cdb);

        const int CdTextBufferSize = 65536;
        byte[] response = ArrayPool<byte>.Shared.Rent(CdTextBufferSize);

        try
        {
            try
            {
                ExecuteScsiCommand(cdb, response.AsSpan(0, CdTextBufferSize));
            }
            catch (OpticalDriveException)
            {
                return Task.FromResult<CdText?>(null);
            }

            CdText? cdText = CdTextCommands.ParseCdText(response.AsSpan(0, CdTextBufferSize));
            return Task.FromResult(cdText);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(response);
        }
    }

    /// <inheritdoc />
    public Task<int> ReadSectorsAsync(
        long lba,
        int count,
        Memory<byte> buffer,
        ReadOptions flags = ReadOptions.None,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        int requiredSize = CdConstants.GetReadBufferSize(flags, count);

        if (buffer.Length < requiredSize)
        {
            throw new ArgumentException(
                $"Buffer too small: {buffer.Length} bytes provided, {requiredSize} required.",
                nameof(buffer));
        }

        ArgumentOutOfRangeException.ThrowIfNegative(lba);

        if (count <= 0)
        {
            return Task.FromResult(0);
        }

        byte[] cdb = new byte[12];
        ScsiCommands.BuildReadCd(cdb, lba, count, flags);

        ExecuteScsiCommand(cdb, buffer.Span.Slice(0, requiredSize));

        return Task.FromResult(count);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        // Release in reverse acquisition order.
        if (_exclusiveAccessHeld && _scsiDeviceInterfacePtr != IntPtr.Zero)
        {
            try
            {
                ReleaseExclusiveAccess(_scsiDeviceInterfacePtr);
            }
#pragma warning disable CA1031 // Dispose must not throw; swallowing any exception is correct here
            catch
            {
                // Best effort — don't mask the primary disposal path.
            }
#pragma warning restore CA1031

            _exclusiveAccessHeld = false;
        }

        ReleaseScsiInterface();

        _service?.Dispose();

        // Remount the disc so the user gets their filesystem view back.
        if (_daDisk is not null && !_daDisk.IsInvalid)
        {
            TryRemount();
        }

        _daDisk?.Dispose();

        if (_daSession is not null && !_daSession.IsInvalid)
        {
            IntPtr runLoop = DiskArbitrationNative.CFRunLoopGetCurrent();
            IntPtr defaultMode = DiskArbitrationNative.GetCFRunLoopDefaultMode();
            DiskArbitrationNative.DASessionUnscheduleFromRunLoop(
                _daSession.DangerousGetHandle(), runLoop, defaultMode);
        }

        _daSession?.Dispose();
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    // ── DiskArbitration unmount / remount ──────────────────────

    private static void UnmountDisk(IntPtr diskPtr, bool remount)
    {
        // Pin a managed delegate for the callback. The delegate captures
        // local state via the context pointer (a GCHandle.ToIntPtr of a
        // state object).
        var state = new DACallbackState();
        GCHandle stateHandle = GCHandle.Alloc(state);

        try
        {
            DADiskCallback callback = DACallback;
            IntPtr callbackPtr = Marshal.GetFunctionPointerForDelegate(callback);

            if (remount)
            {
                DiskArbitrationNative.DADiskMount(
                    diskPtr,
                    IntPtr.Zero,
                    DiskArbitrationNative.kDADiskMountOptionDefault,
                    callbackPtr,
                    GCHandle.ToIntPtr(stateHandle));
            }
            else
            {
                DiskArbitrationNative.DADiskUnmount(
                    diskPtr,
                    DiskArbitrationNative.kDADiskUnmountOptionDefault,
                    callbackPtr,
                    GCHandle.ToIntPtr(stateHandle));
            }

            IntPtr defaultMode = DiskArbitrationNative.GetCFRunLoopDefaultMode();

            int rc = DiskArbitrationNative.CFRunLoopRunInMode(
                defaultMode,
                UnmountTimeoutSeconds,
                returnAfterSourceHandled: false);

            // Force the JIT to treat `callback` as live across the unmanaged
            // call so the delegate isn't GC'd out from under the native code
            // while the run loop is still dispatching events. "The local is
            // in scope" is not the same as "the GC sees it as reachable" —
            // GC.KeepAlive is the documented pattern for exactly this case.
            GC.KeepAlive(callback);

            if (!state.Completed)
            {
                throw new OpticalDriveException(
                    remount
                        ? $"DADiskMount timed out after {UnmountTimeoutSeconds}s."
                        : $"DADiskUnmount timed out after {UnmountTimeoutSeconds}s.");
            }

            if (!state.Succeeded)
            {
                throw new OpticalDriveException(
                    remount
                        ? $"DADiskMount failed: {state.DissenterMessage ?? "unknown error"}."
                        : $"DADiskUnmount failed: {state.DissenterMessage ?? "unknown error"}. " +
                          "The disc may be in use by another process.");
            }
        }
        finally
        {
            stateHandle.Free();
        }
    }

    private void TryRemount()
    {
        try
        {
            UnmountDisk(_daDisk.DangerousGetHandle(), remount: true);
        }
#pragma warning disable CA1031 // Best-effort remount — if it fails, the disc is simply left unmounted
        catch
        {
            // Don't mask the primary exception path.
        }
#pragma warning restore CA1031
    }

    private static void DACallback(IntPtr disk, IntPtr dissenter, IntPtr context)
    {
        var state = (DACallbackState)GCHandle.FromIntPtr(context).Target!;
        state.Completed = true;

        if (dissenter == IntPtr.Zero)
        {
            state.Succeeded = true;
        }
        else
        {
            IntPtr messagePtr = DiskArbitrationNative.DADissenterGetStatusString(dissenter);

            if (messagePtr != IntPtr.Zero)
            {
                // CFStringRef — converting to managed string requires more
                // CoreFoundation plumbing than we want to drag in. For now,
                // record that a dissenter was present and leave the text
                // out of the message. If the specific dissenter text turns
                // out to matter during hardware testing, we can add a
                // CFString-to-managed helper then.
                state.DissenterMessage = "DiskArbitration dissenter present";
            }
        }

        DiskArbitrationNative.CFRunLoopStop(DiskArbitrationNative.CFRunLoopGetCurrent());
    }

    private sealed class DACallbackState
    {
        public bool Completed;
        public bool Succeeded;
        public string? DissenterMessage;
    }

    // ── IOKit service discovery ────────────────────────────────

    private static IntPtr FindIoService(string bsdName)
    {
        IntPtr matching = IoKitNative.IOBSDNameMatching(
            IoKitNative.kIOMasterPortDefault, 0, bsdName);

        if (matching == IntPtr.Zero)
        {
            throw new OpticalDriveException(
                $"IOBSDNameMatching returned NULL for BSD name '{bsdName}'.");
        }

        // IOServiceGetMatchingServices consumes the matching dict — do NOT
        // CFRelease it, even on failure.
        int kr = IoKitNative.IOServiceGetMatchingServices(
            IoKitNative.kIOMasterPortDefault, matching, out IntPtr iterator);

        if (kr != 0)
        {
            throw new OpticalDriveException(
                $"IOServiceGetMatchingServices failed: 0x{kr:X8}.");
        }

        try
        {
            IntPtr service = IoKitNative.IOIteratorNext(iterator);

            if (service == IntPtr.Zero)
            {
                throw new OpticalDriveException(
                    $"No IOKit service found for BSD name '{bsdName}'.");
            }

            return service;
        }
        finally
        {
            _ = IoKitNative.IOObjectRelease(iterator);
        }
    }

    // ── SCSI plug-in interface loading ─────────────────────────

    private static unsafe IntPtr LoadScsiInterface(IntPtr service)
    {
        IntPtr plugInInterface = IntPtr.Zero;

        fixed (byte* pluginTypePtr = IoKitNative.kIOSCSITaskDeviceUserClientTypeID)
        fixed (byte* interfaceTypePtr = IoKitNative.kIOCFPlugInInterfaceID)
        {
            int rc = IoKitNative.IOCreatePlugInInterfaceForService(
                service,
                pluginTypePtr,
                interfaceTypePtr,
                out plugInInterface,
                out int _);

            if (rc != 0 || plugInInterface == IntPtr.Zero)
            {
                throw new OpticalDriveException(
                    $"IOCreatePlugInInterfaceForService failed: 0x{rc:X8}.");
            }
        }

        try
        {
            // Now QueryInterface on the plug-in to get the SCSITaskDeviceInterface.
            // The plug-in vtable has the standard IUnknown header: slot 1 is
            // QueryInterface. Dereference twice: read the vtable pointer,
            // then read QueryInterface out of the struct.
            IntPtr scsiInterface = QueryScsiInterface(plugInInterface);

            if (scsiInterface == IntPtr.Zero)
            {
                throw new OpticalDriveException(
                    "QueryInterface for SCSITaskDeviceInterface returned NULL.");
            }

            return scsiInterface;
        }
        finally
        {
            // Release the base plug-in; we're keeping the secondary SCSI interface.
            InvokeIUnknownRelease(plugInInterface);
        }
    }

    private static unsafe IntPtr QueryScsiInterface(IntPtr plugInInterfacePtr)
    {
        // plugInInterfacePtr is an IOCFPlugInInterface**.
        // Dereference once to get the vtable pointer.
        IntPtr vtable = Marshal.ReadIntPtr(plugInInterfacePtr);

        // IUnknown layout: offset 0 = _reserved, 8 = QueryInterface.
        // QueryInterface signature: HRESULT (*)(void* self, CFUUIDBytes iid, void** ppv)
        // where CFUUIDBytes is passed BY VALUE (16 bytes). On x86_64 System V
        // calling convention, structs up to 16 bytes pass in registers, so
        // we declare it as two 8-byte values.
        IntPtr queryInterfacePtr = Marshal.ReadIntPtr(vtable, 8);

        var queryInterface = (delegate* unmanaged[Cdecl]<IntPtr, long, long, IntPtr*, int>)queryInterfacePtr;

        IntPtr scsiInterface = IntPtr.Zero;

        // Pack the 16-byte CFUUIDBytes into two longs to match the register-passing ABI.
        ReadOnlySpan<byte> uuid = IoKitNative.kIOSCSITaskDeviceInterfaceID;
        long uuidLow = MemoryMarshal.Read<long>(uuid);
        long uuidHigh = MemoryMarshal.Read<long>(uuid.Slice(8));

        int hr = queryInterface(plugInInterfacePtr, uuidLow, uuidHigh, &scsiInterface);

        if (hr != 0)
        {
            throw new OpticalDriveException(
                $"QueryInterface for SCSITaskDeviceInterface failed: HRESULT 0x{hr:X8}.");
        }

        return scsiInterface;
    }

    private static unsafe void InvokeIUnknownRelease(IntPtr interfacePtr)
    {
        if (interfacePtr == IntPtr.Zero)
        {
            return;
        }

        // Read vtable pointer, then the Release slot at offset 24.
        IntPtr vtable = Marshal.ReadIntPtr(interfacePtr);
        IntPtr releasePtr = Marshal.ReadIntPtr(vtable, 24);

        var release = (delegate* unmanaged[Cdecl]<IntPtr, uint>)releasePtr;
        release(interfacePtr);
    }

    private static unsafe void ObtainExclusiveAccess(IntPtr scsiInterfacePtr)
    {
        IntPtr vtable = Marshal.ReadIntPtr(scsiInterfacePtr);
        IntPtr obtainPtr = Marshal.ReadIntPtr(vtable, 64); // offset from IoKitNative vtable layout

        var obtain = (delegate* unmanaged[Cdecl]<IntPtr, int>)obtainPtr;
        int rc = obtain(scsiInterfacePtr);

        if (rc != IoKitNative.kIOReturnSuccess)
        {
            throw MapIoReturn(rc, "ObtainExclusiveAccess");
        }
    }

    private static unsafe void ReleaseExclusiveAccess(IntPtr scsiInterfacePtr)
    {
        IntPtr vtable = Marshal.ReadIntPtr(scsiInterfacePtr);
        IntPtr releasePtr = Marshal.ReadIntPtr(vtable, 72);

        var release = (delegate* unmanaged[Cdecl]<IntPtr, int>)releasePtr;
        release(scsiInterfacePtr);
    }

    private void ReleaseScsiInterface()
    {
        if (_scsiDeviceInterfacePtr != IntPtr.Zero)
        {
            InvokeIUnknownRelease(_scsiDeviceInterfacePtr);
            _scsiDeviceInterfacePtr = IntPtr.Zero;
        }
    }

    // ── SCSI command execution via the task interface ──────────

    private DriveInquiry QueryInquiry()
    {
        byte[] cdb = new byte[6];
        ScsiCommands.BuildInquiry(cdb);

        byte[] response = new byte[ScsiCommands.InquiryResponseLength];
        ExecuteScsiCommand(cdb, response.AsSpan());

        return ScsiCommands.ParseInquiry(response);
    }

    private unsafe void ExecuteScsiCommand(ReadOnlySpan<byte> cdb, Span<byte> dataBuffer)
    {
        // Step 1: CreateSCSITask via the device interface vtable.
        IntPtr deviceVtable = Marshal.ReadIntPtr(_scsiDeviceInterfacePtr);
        IntPtr createTaskPtr = Marshal.ReadIntPtr(deviceVtable, 80);

        var createTask = (delegate* unmanaged[Cdecl]<IntPtr, IntPtr>)createTaskPtr;
        IntPtr taskPtr = createTask(_scsiDeviceInterfacePtr);

        if (taskPtr == IntPtr.Zero)
        {
            throw new OpticalDriveException("CreateSCSITask returned NULL.");
        }

        try
        {
            // Step 2: Configure and execute the task.
            IntPtr taskVtable = Marshal.ReadIntPtr(taskPtr);

            Span<byte> senseBuffer = stackalloc byte[SenseBufferSize];

            fixed (byte* cdbPtr = cdb)
            fixed (byte* dataPtr = dataBuffer)
            fixed (byte* sensePtr = senseBuffer)
            {
                // SetCommandDescriptorBlock at offset 64
                var setCdb = (delegate* unmanaged[Cdecl]<IntPtr, byte*, byte, int>)Marshal.ReadIntPtr(taskVtable, 64);
                int rc = setCdb(taskPtr, cdbPtr, (byte)cdb.Length);

                if (rc != IoKitNative.kIOReturnSuccess)
                {
                    throw MapIoReturn(rc, "SetCommandDescriptorBlock");
                }

                // SetScatterGatherEntries at offset 88
                var range = new IOVirtualRange
                {
                    Address = (IntPtr)dataPtr,
                    Length = (IntPtr)dataBuffer.Length,
                };

                var setSg = (delegate* unmanaged[Cdecl]<IntPtr, IOVirtualRange*, byte, ulong, byte, int>)Marshal.ReadIntPtr(taskVtable, 88);
                rc = setSg(taskPtr, &range, 1, (ulong)dataBuffer.Length, IoKitNative.kSCSIDataTransfer_FromTargetToInitiator);

                if (rc != IoKitNative.kIOReturnSuccess)
                {
                    throw MapIoReturn(rc, "SetScatterGatherEntries");
                }

                // SetTimeoutDuration at offset 96
                var setTimeout = (delegate* unmanaged[Cdecl]<IntPtr, uint, int>)Marshal.ReadIntPtr(taskVtable, 96);
                rc = setTimeout(taskPtr, DefaultTimeoutMs);

                if (rc != IoKitNative.kIOReturnSuccess)
                {
                    throw MapIoReturn(rc, "SetTimeoutDuration");
                }

                // ExecuteTaskSync at offset 128
                byte taskStatus;
                ulong actualTransferred;

                var executeSync = (delegate* unmanaged[Cdecl]<IntPtr, byte*, byte*, ulong*, int>)Marshal.ReadIntPtr(taskVtable, 128);
                rc = executeSync(taskPtr, sensePtr, &taskStatus, &actualTransferred);

                if (rc != IoKitNative.kIOReturnSuccess)
                {
                    throw MapIoReturn(rc, "ExecuteTaskSync");
                }

                // Check SCSI-level status. GOOD = 0x00 succeeds; CHECK CONDITION
                // (0x02) means sense data is valid and we route through the
                // shared sense-data parser.
                if (taskStatus == IoKitNative.kSCSITaskStatus_GOOD)
                {
                    return;
                }

                if (taskStatus == IoKitNative.kSCSITaskStatus_CHECK_CONDITION)
                {
                    // Some drivers populate the sense buffer via the
                    // ExecuteTaskSync call directly; others require an
                    // explicit GetAutoSenseData. Try both: if the sense
                    // buffer from ExecuteTaskSync is non-zero at byte 0
                    // (response code != 0), use it. Otherwise query.
                    if (sensePtr[0] == 0)
                    {
                        var getAutoSense = (delegate* unmanaged[Cdecl]<IntPtr, byte*, byte, int>)Marshal.ReadIntPtr(taskVtable, 176);
                        getAutoSense(taskPtr, sensePtr, SenseBufferSize);
                    }

                    throw ScsiCommands.MapSenseData(senseBuffer);
                }

                throw new OpticalDriveException(
                    $"SCSI task returned status 0x{taskStatus:X2} (not GOOD, not CHECK_CONDITION).");
            }
        }
        finally
        {
            InvokeIUnknownRelease(taskPtr);
        }
    }

    private static OpticalDriveException MapIoReturn(int ioReturn, string operation)
    {
        return ioReturn switch
        {
            IoKitNative.kIOReturnNoMedia => new MediaNotPresentException(
                $"{operation}: no media in drive."),
            IoKitNative.kIOReturnNotReady => new DriveNotReadyException(
                $"{operation}: drive not ready."),
            IoKitNative.kIOReturnBusy => new OpticalDriveException(
                $"{operation}: device busy (is the disc still mounted?)."),
            IoKitNative.kIOReturnExclusiveAccess => new OpticalDriveException(
                $"{operation}: another process holds exclusive access."),
            IoKitNative.kIOReturnNotPermitted => new OpticalDriveException(
                $"{operation}: permission denied."),
            _ => new OpticalDriveException(
                $"{operation} failed: IOReturn 0x{ioReturn:X8}."),
        };
    }
}
