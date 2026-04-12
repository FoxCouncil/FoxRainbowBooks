namespace FoxRedbook.Tests;

/// <summary>
/// Test collection that serializes all hardware tests. Prevents xUnit
/// from running <see cref="HardwareTests"/> and <see cref="HardwareLongTests"/>
/// concurrently on separate thread pool workers, which triggers a SIGSEGV
/// in IOKit's non-thread-safe error-logging path on macOS when two threads
/// call <c>IOCreatePlugInInterfaceForService</c> on the same service
/// simultaneously. Affects macOS specifically; Windows and Linux don't
/// exhibit the race in practice, but serializing on all platforms is
/// harmless and keeps behavior consistent.
/// </summary>
[CollectionDefinition(nameof(SerialHardware), DisableParallelization = true)]
public sealed class SerialHardware;
