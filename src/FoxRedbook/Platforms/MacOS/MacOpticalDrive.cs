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
public sealed class MacOpticalDrive : IOpticalDrive, IScsiTransport
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
    // Both must stay alive for the lifetime of the drive: the SCSI device
    // interface shares the underlying Mach user-client port with the MMC
    // interface. Releasing the MMC interface early destroys the shared port
    // and causes MACH_SEND_INVALID_DEST on subsequent SCSI calls.
    private IntPtr _mmcInterfacePtr;          // MMCDeviceInterface**
    private IntPtr _scsiDeviceInterfacePtr;   // SCSITaskDeviceInterface**
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

        // Route DA callbacks to a libdispatch global queue rather than the calling
        // thread's CFRunLoop. The legacy run-loop path deadlocks when this code is
        // invoked from inside a host that already owns the main CFRunLoop (Avalonia,
        // any Cocoa app): the unmount callback never gets dispatched because the
        // outer run loop is in a mode that doesn't service our DA source. The dispatch
        // queue path is independent of the caller's thread and works in any host.
        IntPtr daQueue = DiskArbitrationNative.dispatch_get_global_queue(
            DiskArbitrationNative.DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
        DiskArbitrationNative.DASessionSetDispatchQueue(sessionPtr, daQueue);

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
            UnmountDisk(diskPtr, _bsdName, remount: false);

            // Phase 2: IOKit service discovery from the BSD name.
            IntPtr servicePtr = FindIoService(_bsdName);
            _service = new SafeIoObjectHandle(servicePtr);

            // Phase 3: Load the SCSI plug-in interface and claim exclusive access.
            var interfaces = LoadScsiInterface(servicePtr);
            _mmcInterfacePtr = interfaces.MmcInterface;
            _scsiDeviceInterfacePtr = interfaces.ScsiDeviceInterface;

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
            DiskArbitrationNative.DASessionSetDispatchQueue(sessionPtr, IntPtr.Zero);
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
            // Detach the dispatch queue before releasing the session — pairs with
            // the DASessionSetDispatchQueue call in the constructor.
            DiskArbitrationNative.DASessionSetDispatchQueue(_daSession.DangerousGetHandle(), IntPtr.Zero);
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

    // DA returns kDAReturnNotPrivileged (0xF8DA0009) when an unprivileged user
    // tries to unmount media auto-mounted by launchd (the common case for an
    // audio CD inserted while the app is running). diskutil(8) has a setuid-root
    // helper that handles exactly this case, so we fall back to shelling out.
    private const uint kDAReturnNotPrivileged = 0xF8DA0009;

    private static void UnmountDisk(IntPtr diskPtr, string bsdName, bool remount)
    {
        // Callback fires on the libdispatch worker thread the session was bound to.
        // We wait on a ManualResetEventSlim rather than spinning a CFRunLoop, so this
        // method works from any caller — including hosts whose main thread already
        // owns a run loop (Avalonia/Cocoa). The callback delegate captures local
        // state via a GCHandle.ToIntPtr context.
        var state = new DACallbackState();
        var done = new ManualResetEventSlim(initialState: false);
        state.Signal = done;
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
                // Use kDADiskUnmountOptionWhole to cascade the unmount to any
                // filesystem children. Hybrid audio/data CDs (e.g. an audio
                // session plus an Apple_HFS or ISO9660 partition) fail with an
                // opaque dissenter error under kDADiskUnmountOptionDefault
                // because the child partition is mounted independently.
                DiskArbitrationNative.DADiskUnmount(
                    diskPtr,
                    DiskArbitrationNative.kDADiskUnmountOptionWhole,
                    callbackPtr,
                    GCHandle.ToIntPtr(stateHandle));
            }

            bool completed = done.Wait(TimeSpan.FromSeconds(UnmountTimeoutSeconds));

            // Keep the delegate reachable until after libdispatch has finished
            // delivering callbacks for this request. "The local is in scope" is not
            // equivalent to "the GC sees it as reachable across the unmanaged
            // boundary" — GC.KeepAlive is the documented pattern.
            GC.KeepAlive(callback);

            if (!completed)
            {
                throw new OpticalDriveException(
                    remount
                        ? $"DADiskMount timed out after {UnmountTimeoutSeconds}s."
                        : $"DADiskUnmount timed out after {UnmountTimeoutSeconds}s.");
            }

            if (!state.Succeeded)
            {
                if (state.DissenterStatus == kDAReturnNotPrivileged
                    && TryDiskutilFallback(bsdName, remount))
                {
                    return;
                }

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
            done.Dispose();
        }
    }

    private static bool TryDiskutilFallback(string bsdName, bool remount)
    {
        try
        {
            using var p = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
            {
                FileName = "/usr/sbin/diskutil",
                ArgumentList = { remount ? "mount" : "unmount", bsdName },
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
            if (p is null || !p.WaitForExit((int)TimeSpan.FromSeconds(UnmountTimeoutSeconds).TotalMilliseconds) || p.ExitCode != 0)
            {
                return false;
            }

            // diskutil unmount detaches cddafs, which triggers a brief IOKit
            // re-publish cycle on the partition scheme. If we race ahead to
            // IOBSDNameMatching + IORegistryEntryGetParentEntry while that's
            // still settling, the IOService handle we get back is stale and
            // IOCreatePlugInInterfaceForService later returns kIOReturnUnsupported.
            // Wait for the tree to quiesce. 250ms is empirically sufficient on
            // an Apple Silicon Mac with a Pioneer BD-RW.
            System.Threading.Thread.Sleep(250);
            return true;
        }
#pragma warning disable CA1031 // Fallback is best-effort: any failure means we report the original DA error.
        catch
        {
            return false;
        }
#pragma warning restore CA1031
    }

    private void TryRemount()
    {
        try
        {
            UnmountDisk(_daDisk.DangerousGetHandle(), _bsdName, remount: true);
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
            state.DissenterStatus = DiskArbitrationNative.DADissenterGetStatus(dissenter);
            state.DissenterMessage = GetDissenterMessage(dissenter);
        }

        state.Signal?.Set();
    }

    private static unsafe string GetDissenterMessage(IntPtr dissenter)
    {
        // Always extract the numeric status code — it's present on every
        // dissenter, unlike the string which Apple's headers mark __nullable.
        uint status = DiskArbitrationNative.DADissenterGetStatus(dissenter);

        IntPtr cfString = DiskArbitrationNative.DADissenterGetStatusString(dissenter);

        if (cfString == IntPtr.Zero)
        {
            return $"DA status 0x{status:X8}";
        }

        Span<byte> buffer = stackalloc byte[512];

        fixed (byte* pBuf = buffer)
        {
            bool ok = DiskArbitrationNative.CFStringGetCString(
                cfString,
                (IntPtr)pBuf,
                buffer.Length,
                DiskArbitrationNative.kCFStringEncodingUTF8);

            if (!ok)
            {
                return $"DA status 0x{status:X8}";
            }

            int len = buffer.IndexOf((byte)0);

            if (len < 0)
            {
                len = buffer.Length;
            }

            string message = System.Text.Encoding.UTF8.GetString(buffer[..len]);
            return $"{message} (DA status 0x{status:X8})";
        }
    }

    private sealed class DACallbackState
    {
        public bool Completed;
        public bool Succeeded;
        public string? DissenterMessage;
        public uint DissenterStatus;
        public ManualResetEventSlim? Signal;
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

        IntPtr mediaService;

        try
        {
            mediaService = IoKitNative.IOIteratorNext(iterator);

            if (mediaService == IntPtr.Zero)
            {
                throw new OpticalDriveException(
                    $"No IOKit service found for BSD name '{bsdName}'.");
            }
        }
        finally
        {
            _ = IoKitNative.IOObjectRelease(iterator);
        }

        // The matching dictionary for a BSD name yields an IOMedia object, but
        // the SCSI Task plugin interface is exposed by the SCSI peripheral
        // device that is its IOService-plane ancestor. Walk up until we find
        // the services node (IOCompactDiscServices / IODVDServices / IOBDServices)
        // that exposes the IOCFPlugInTypes dictionary needed by
        // IOCreatePlugInInterfaceForService. The device nub
        // (IOSCSIPeripheralDeviceType05) sits above the services node and does
        // NOT carry the plug-in dictionary.
        return FindScsiPeripheralAncestor(mediaService);
    }

    /// <summary>
    /// Walks the IOService plane from <paramref name="mediaService"/> upward,
    /// looking for an ancestor that is one of the optical-disc services classes
    /// (<c>IOCompactDiscServices</c>, <c>IODVDServices</c>, or
    /// <c>IOBDServices</c>). These are parallel siblings, not an inheritance
    /// chain, so we must check each class name individually. Returns the
    /// ancestor (retained) and releases <paramref name="mediaService"/>; or
    /// returns <paramref name="mediaService"/> itself if no matching ancestor
    /// is found.
    /// </summary>
    private static IntPtr FindScsiPeripheralAncestor(IntPtr mediaService)
    {
        IntPtr current = mediaService;

        while (true)
        {
            if (IsOpticalServicesNode(current))
            {
                // Found the services node (IOCompactDiscServices / IODVDServices
                // / IOBDServices) that owns the IOCFPlugInTypes dictionary.
                // If it's not the original IOMedia service, release the IOMedia
                // entry since we're returning a different (retained) object.
                if (current != mediaService)
                {
                    _ = IoKitNative.IOObjectRelease(mediaService);
                }

                return current;
            }

            int kr = IoKitNative.IORegistryEntryGetParentEntry(
                current, IoKitNative.kIOServicePlane, out IntPtr parent);

            if (kr != 0 || parent == IntPtr.Zero)
            {
                // Reached the root without finding a SCSI peripheral ancestor.
                // Release any intermediate entry and fall back to the original
                // IOMedia service; the plugin load will fail cleanly (or crash
                // in IOKit's log path on macOS 26, which we can't prevent but
                // at least the error path is documented).
                if (current != mediaService)
                {
                    _ = IoKitNative.IOObjectRelease(current);
                }

                return mediaService;
            }

            // Release the intermediate entry (unless it's the original
            // mediaService, which we hold as a fallback).
            if (current != mediaService)
            {
                _ = IoKitNative.IOObjectRelease(current);
            }

            current = parent;
        }
    }

    private static bool IsOpticalServicesNode(IntPtr ioObject)
    {
        foreach (string className in IoKitNative.OpticalServicesClasses)
        {
            if (IoKitNative.IOObjectConformsTo(ioObject, className))
            {
                return true;
            }
        }

        return false;
    }

    // ── SCSI plug-in interface loading ─────────────────────────

    /// <summary>
    /// Both the MMC and SCSI device interfaces returned from the plug-in
    /// loading pipeline. Both must be kept alive for the drive's lifetime
    /// because they share the underlying Mach user-client port.
    /// </summary>
    private readonly record struct ScsiInterfaces(IntPtr MmcInterface, IntPtr ScsiDeviceInterface);

    private static ScsiInterfaces LoadScsiInterface(IntPtr service)
    {
        // IOCreatePlugInInterfaceForService takes CFUUIDRef objects (opaque
        // CoreFoundation pointers), NOT raw byte arrays. Create proper
        // CFUUIDRef objects from the 16-byte UUID constants, use them, and
        // release them afterwards.
        IntPtr pluginTypeUuid = CreateCFUUID(IoKitNative.kIOMMCDeviceUserClientTypeID);
        IntPtr interfaceTypeUuid = CreateCFUUID(IoKitNative.kIOCFPlugInInterfaceID);

        IntPtr plugInInterface = IntPtr.Zero;

        try
        {
            // When DA's unmount fails and we fell back to diskutil(8), macOS auto-mount
            // races to re-claim the disc. The window during which Finder is re-grabbing
            // SCSITaskUserClient via SCSITaskUserClientIniter (we can see the matched
            // category in ioreg) makes IOCreatePlugInInterfaceForService return
            // kIOReturnUnsupported transiently. Retrying with brief backoff catches the
            // moment between automount publishing the IOMedia and Finder's match win.
            int rc = 0;
            for (int attempt = 0; attempt < 8; attempt++)
            {
                rc = IoKitNative.IOCreatePlugInInterfaceForService(
                    service,
                    pluginTypeUuid,
                    interfaceTypeUuid,
                    out plugInInterface,
                    out int _);

                if (rc == 0 && plugInInterface != IntPtr.Zero)
                {
                    break;
                }

                System.Threading.Thread.Sleep(150);
            }

            if (rc != 0 || plugInInterface == IntPtr.Zero)
            {
                throw new OpticalDriveException(
                    $"IOCreatePlugInInterfaceForService failed: 0x{rc:X8}.");
            }
        }
        finally
        {
            DiskArbitrationNative.CFRelease(pluginTypeUuid);
            DiskArbitrationNative.CFRelease(interfaceTypeUuid);
        }

        try
        {
            // QueryInterface for the MMCDeviceInterface, then call its
            // GetSCSITaskDeviceInterface method to get the raw SCSI
            // passthrough interface. QueryInterface takes REFIID (CFUUIDBytes)
            // by value — the two-long register-passing pattern handles this.
            ScsiInterfaces interfaces = QueryScsiInterface(plugInInterface);

            if (interfaces.ScsiDeviceInterface == IntPtr.Zero)
            {
                throw new OpticalDriveException(
                    "Failed to obtain SCSITaskDeviceInterface.");
            }

            return interfaces;
        }
        finally
        {
            // Release the base plug-in (IOCFPlugInInterface). This is the
            // CFPlugIn base and is independent of the MMC→SCSI chain —
            // releasing it after QueryInterface succeeds is correct COM
            // convention. Only the MMC→SCSI chain has the shared-port issue.
            InvokeIUnknownRelease(plugInInterface);
        }
    }

    private static IntPtr CreateCFUUID(ReadOnlySpan<byte> uuidBytes)
    {
        var bytes = CFUUIDBytes.FromSpan(uuidBytes);
        IntPtr cfUuid = DiskArbitrationNative.CFUUIDCreateFromUUIDBytes(IntPtr.Zero, bytes);

        if (cfUuid == IntPtr.Zero)
        {
            throw new OpticalDriveException("CFUUIDCreateFromUUIDBytes returned NULL.");
        }

        return cfUuid;
    }

    /// <summary>
    /// Obtains an <c>MMCDeviceInterface**</c> and a
    /// <c>SCSITaskDeviceInterface**</c> from the IOCFPlugInInterface.
    /// The MMC plugin loaded via <c>kIOMMCDeviceUserClientTypeID</c> does not
    /// directly support <c>kIOSCSITaskDeviceInterfaceID</c> — it only supports
    /// <c>kIOMMCDeviceInterfaceID</c>. The MMC interface exposes a
    /// <c>GetSCSITaskDeviceInterface</c> method (vtable slot 13, offset 136)
    /// that returns the raw SCSI passthrough interface.
    /// </summary>
    /// <remarks>
    /// The returned MMC interface must be kept alive until the SCSI device
    /// interface is released. The two share the underlying Mach user-client
    /// port; releasing the MMC interface early causes
    /// <c>MACH_SEND_INVALID_DEST</c> (0x10000003) on subsequent SCSI calls.
    /// </remarks>
    private static unsafe ScsiInterfaces QueryScsiInterface(IntPtr plugInInterfacePtr)
    {
        // Step 1: QueryInterface for kIOMMCDeviceInterfaceID → MMCDeviceInterface**
        IntPtr vtable = Marshal.ReadIntPtr(plugInInterfacePtr);
        IntPtr queryInterfacePtr = Marshal.ReadIntPtr(vtable, 8);

        var queryInterface = (delegate* unmanaged[Cdecl]<IntPtr, long, long, IntPtr*, int>)queryInterfacePtr;

        IntPtr mmcInterface = IntPtr.Zero;

        ReadOnlySpan<byte> uuid = IoKitNative.kIOMMCDeviceInterfaceID;
        long uuidLow = MemoryMarshal.Read<long>(uuid);
        long uuidHigh = MemoryMarshal.Read<long>(uuid.Slice(8));

        int hr = queryInterface(plugInInterfacePtr, uuidLow, uuidHigh, &mmcInterface);

        if (hr != 0)
        {
            throw new OpticalDriveException(
                $"QueryInterface for MMCDeviceInterface failed: HRESULT 0x{hr:X8}.");
        }

        // Step 2: Call GetSCSITaskDeviceInterface through the MMC vtable.
        // MMCDeviceInterface layout: IUNKNOWN_C_GUTS (4 × 8) + version/revision
        // (4 + 4 padding) + 13 method slots. GetSCSITaskDeviceInterface is
        // slot 13 at offset 136.
        // Signature: SCSITaskDeviceInterface** (*)(void* self)
        IntPtr mmcVtable = Marshal.ReadIntPtr(mmcInterface);
        IntPtr getSCSITaskDevicePtr = Marshal.ReadIntPtr(mmcVtable, 136);

        var getSCSITaskDevice = (delegate* unmanaged[Cdecl]<IntPtr, IntPtr>)getSCSITaskDevicePtr;
        IntPtr scsiInterface = getSCSITaskDevice(mmcInterface);

        if (scsiInterface == IntPtr.Zero)
        {
            InvokeIUnknownRelease(mmcInterface);

            throw new OpticalDriveException(
                "MMCDeviceInterface.GetSCSITaskDeviceInterface returned NULL.");
        }

        return new ScsiInterfaces(mmcInterface, scsiInterface);
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
        // Release inner (SCSI) before outer (MMC) — the SCSI interface
        // shares the Mach user-client port owned by the MMC interface.
        if (_scsiDeviceInterfacePtr != IntPtr.Zero)
        {
            InvokeIUnknownRelease(_scsiDeviceInterfacePtr);
            _scsiDeviceInterfacePtr = IntPtr.Zero;
        }

        if (_mmcInterfacePtr != IntPtr.Zero)
        {
            InvokeIUnknownRelease(_mmcInterfacePtr);
            _mmcInterfacePtr = IntPtr.Zero;
        }
    }

    /// <inheritdoc />
    public void Execute(ReadOnlySpan<byte> cdb, Span<byte> buffer, ScsiDirection direction)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        byte transfer = direction switch
        {
            ScsiDirection.None => IoKitNative.kSCSIDataTransfer_NoDataTransfer,
            ScsiDirection.In => IoKitNative.kSCSIDataTransfer_FromTargetToInitiator,
            ScsiDirection.Out => IoKitNative.kSCSIDataTransfer_FromInitiatorToTarget,
            _ => throw new ArgumentOutOfRangeException(nameof(direction)),
        };

        ExecuteScsiCommand(cdb, buffer, transfer);
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

    private unsafe void ExecuteScsiCommand(ReadOnlySpan<byte> cdb, Span<byte> dataBuffer, byte transferDirection = IoKitNative.kSCSIDataTransfer_FromTargetToInitiator)
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
                rc = setSg(taskPtr, &range, 1, (ulong)dataBuffer.Length, transferDirection);

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
