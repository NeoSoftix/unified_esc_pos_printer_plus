import 'dart:async';
import 'dart:typed_data';

import '../core/commands.dart';
import '../exceptions/printer_exception.dart';
import '../models/printer_connection_state.dart';
import '../models/printer_device.dart';
import '../platform/bluetooth_platform_channel.dart';
import 'printer_connector.dart';

/// Connector for BLE (Bluetooth Low Energy) ESC/POS printers.
///
/// **Discovery:** Uses native BLE scanning to find nearby BLE devices.
///
/// **Connection:** Discovers services, then auto-locates a writable
/// characteristic. Tries the well-known ESC/POS BLE service UUID first;
/// falls back to any writable characteristic found.
///
/// **Writing:** Negotiates MTU 512 after connecting and chunks data into
/// MTU-sized write operations. Write-without-response data is drained
/// before [disconnect] closes the link.
///
/// **Permissions:** Automatically requests Bluetooth permissions when
/// scanning or connecting. Throws [PrinterPermissionException] if denied.
class BleConnector extends PrinterConnector<BlePrinterDevice> {
  BleConnector({
    this.writeWithoutResponseDrainDelay =
        const Duration(milliseconds: kBleWwrDrainDelayMs),
  });

  /// Drain delay applied after write-without-response data.
  final Duration writeWithoutResponseDrainDelay;

  final BluetoothPlatformChannel _platform = BluetoothPlatformChannel.instance;

  int _mtuPayload = 20;
  bool _writeWithoutResponse = false;
  bool _pendingWwrDrain = false;
  StreamSubscription<Map<String, dynamic>>? _connectionSub;

  PrinterConnectionState _state = PrinterConnectionState.disconnected;
  final StreamController<PrinterConnectionState> _stateController =
      StreamController<PrinterConnectionState>.broadcast();

  @override
  Stream<PrinterConnectionState> get stateStream => _stateController.stream;

  @override
  PrinterConnectionState get state => _state;

  @override
  Stream<List<BlePrinterDevice>> scan({
    Duration timeout = const Duration(seconds: 5),
  }) async* {
    _setState(PrinterConnectionState.scanning);

    // Request permissions first
    final bool granted = await _platform.requestBluetoothPermissions();
    if (!granted) {
      _setState(PrinterConnectionState.disconnected);
      throw const PrinterPermissionException(
        'Bluetooth permissions were denied',
      );
    }

    final List<BlePrinterDevice> found = [];

    // Emit bonded (paired) BLE devices immediately.
    try {
      final List<Map<String, dynamic>> bonded =
          await _platform.getBondedBleDevices();

      for (final Map<String, dynamic> d in bonded) {
        found.add(BlePrinterDevice(
          name: (d['name'] as String?) ?? (d['deviceId'] as String),
          deviceId: d['deviceId'] as String,
        ));
      }

      if (found.isNotEmpty) yield List<BlePrinterDevice>.from(found);
    } catch (_) {
      // Ignore — permissions may be denied; scan below will also fail.
    }

    final Completer<void> scanDone = Completer<void>();
    StreamSubscription<List<Map<String, dynamic>>>? scanSub;

    try {
      await _platform.startBleScan(
        timeoutMs: timeout.inMilliseconds,
      );
    } catch (e) {
      _setState(PrinterConnectionState.disconnected);

      if (found.isNotEmpty) return;

      throw PrinterScanException('Failed to start BLE scan', cause: e);
    }

    scanSub = _platform.bleScanResults.listen(
      (devices) {
        for (final Map<String, dynamic> d in devices) {
          final String id = d['deviceId'] as String;
          final String name = (d['name'] as String?) ?? id;
          final int existing = found.indexWhere((dev) => dev.deviceId == id);
          if (existing == -1) {
            found.add(BlePrinterDevice(name: name, deviceId: id));
          } else if (name != id && found[existing].name == id) {
            // The native side resolved a friendly name after first emitting
            // the device with its address as a placeholder.
            found[existing] = BlePrinterDevice(name: name, deviceId: id);
          }
        }
      },
      onError: (e) {
        if (!scanDone.isCompleted) scanDone.complete();
      },
      onDone: () {
        if (!scanDone.isCompleted) scanDone.complete();
      },
    );

    // Wait for scan timeout
    await Future.any([
      scanDone.future,
      Future.delayed(timeout),
    ]);

    await scanSub.cancel();
    _setState(PrinterConnectionState.disconnected);

    if (found.isNotEmpty) yield found;
  }

  @override
  Future<void> stopScan() async {
    try {
      await _platform.stopBleScan();
    } catch (_) {
      // Ignore errors from stopBleScan, as it may be called after a failed startBleScan
    }

    if (_state == PrinterConnectionState.scanning) {
      _setState(PrinterConnectionState.disconnected);
    }
  }

  @override
  Future<void> connect(
    BlePrinterDevice device, {
    Duration timeout = const Duration(seconds: 10),
  }) async {
    _assertState(PrinterConnectionState.disconnected, 'connect');
    _setState(PrinterConnectionState.connecting);

    // Request permissions
    final bool granted = await _platform.requestBluetoothPermissions();
    if (!granted) {
      _setState(PrinterConnectionState.error);
      _setState(PrinterConnectionState.disconnected);
      throw const PrinterPermissionException(
        'Bluetooth permissions were denied',
      );
    }

    try {
      await _platform.bleConnect(
        deviceId: device.deviceId,
        timeoutMs: timeout.inMilliseconds,
        serviceUuid: device.serviceUuid,
        characteristicUuid: device.txCharacteristicUuid,
      );
    } catch (e) {
      _setState(PrinterConnectionState.error);
      _setState(PrinterConnectionState.disconnected);
      throw PrinterConnectionException(
        'BLE connection to ${device.name} failed',
        cause: e,
      );
    }

    // Get negotiated MTU
    try {
      _mtuPayload = await _platform.bleGetMtu();
    } catch (_) {
      _mtuPayload = 20; // safe minimum
    }

    // Check write-without-response support
    try {
      _writeWithoutResponse = await _platform.bleSupportsWriteWithoutResponse();
    } catch (_) {
      _writeWithoutResponse = false;
    }

    // Monitor for remote disconnection
    _connectionSub = _platform.connectionStateStream
        .where((event) => event['type'] == 'ble')
        .listen((event) {
      if (event['state'] == 'disconnected' &&
          _state != PrinterConnectionState.disconnected) {
        _setState(PrinterConnectionState.error);
        _setState(PrinterConnectionState.disconnected);
      }
    });

    _setState(PrinterConnectionState.connected);
  }

  @override
  Future<void> writeBytes(List<int> bytes) async {
    _assertState(PrinterConnectionState.connected, 'writeBytes');

    _setState(PrinterConnectionState.printing);

    try {
      for (int i = 0; i < bytes.length; i += _mtuPayload) {
        final int end = (i + _mtuPayload).clamp(0, bytes.length);
        await _platform.bleWrite(
          data: Uint8List.fromList(bytes.sublist(i, end)),
          withoutResponse: _writeWithoutResponse,
        );
      }

      if (_writeWithoutResponse) _pendingWwrDrain = true;
      _setState(PrinterConnectionState.connected);
    } catch (e) {
      _pendingWwrDrain = false;
      _setState(PrinterConnectionState.error);
      _setState(PrinterConnectionState.disconnected);
      throw PrinterWriteException('BLE write failed', cause: e);
    }
  }

  @override
  Future<void> waitWriteComplete() async {
    if (!_pendingWwrDrain) return;
    await Future<void>.delayed(writeWithoutResponseDrainDelay);
    _pendingWwrDrain = false;
  }

  @override
  Future<void> disconnect() async {
    if (_state == PrinterConnectionState.disconnected) return;

    // Cancelling the connection drops chunks still queued in the BLE stack.
    if (_state == PrinterConnectionState.connected) {
      await waitWriteComplete();
    }

    _pendingWwrDrain = false;
    _setState(PrinterConnectionState.disconnecting);

    await _connectionSub?.cancel();
    _connectionSub = null;

    try {
      await _platform.bleDisconnect();
    } finally {
      _setState(PrinterConnectionState.disconnected);
    }
  }

  @override
  Future<void> dispose() async {
    await stopScan();
    await disconnect();
    await _stateController.close();
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  void _setState(PrinterConnectionState next) {
    _state = next;
    if (!_stateController.isClosed) _stateController.add(next);
  }

  void _assertState(PrinterConnectionState required, String operation) {
    if (_state != required) {
      throw PrinterStateException(
        'Cannot $operation: expected $required but was $_state',
        currentState: _state,
        requiredState: required,
      );
    }
  }
}
