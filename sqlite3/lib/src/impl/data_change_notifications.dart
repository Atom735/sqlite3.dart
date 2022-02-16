part of 'implementation.dart';

int _id = 0;
final Map<int, List<MultiStreamController<SqliteUpdate>>> _listeners = {};
final Map<int, List<void Function(SqliteUpdate)>> _callbacks = {};

void _updateCallback(Pointer<Void> data, int kind, Pointer<sqlite3_char> db,
    Pointer<sqlite3_char> table, int rowid) {
  SqliteUpdateKind updateKind;

  switch (kind) {
    case SQLITE_INSERT:
      updateKind = SqliteUpdateKind.insert;
      break;
    case SQLITE_UPDATE:
      updateKind = SqliteUpdateKind.update;
      break;
    case SQLITE_DELETE:
      updateKind = SqliteUpdateKind.delete;
      break;
    default:
      return;
  }

  final tableName = table.readString();
  final update = SqliteUpdate(updateKind, tableName, rowid);
  final listeners = _listeners[data.address];
  final callbacks = _callbacks[data.address];

  if (listeners != null) {
    for (final listener in listeners) {
      listener.add(update);
    }
  }

  if (callbacks != null) {
    for (final callback in callbacks) {
      callback(update);
    }
  }
}

final Pointer<NativeType> _updateCallbackPtr = Pointer.fromFunction<
    Void Function(Pointer<Void>, Int32, Pointer<sqlite3_char>,
        Pointer<sqlite3_char>, Int64)>(_updateCallback);

class _DatabaseUpdates {
  final DatabaseImpl impl;
  final int id = _id++;
  final List<MultiStreamController<SqliteUpdate>> listeners = [];
  final List<void Function(SqliteUpdate)> callbacks = [];
  bool closed = false;

  _DatabaseUpdates(this.impl);

  Stream<SqliteUpdate> get updates {
    return Stream.multi((listener) {
      if (closed) {
        listener.closeSync();
        return;
      }

      addListener(listener);

      listener
        ..onPause = (() => removeListener(listener))
        ..onResume = (() => addListener(listener))
        ..onCancel = (() => removeListener(listener));
    }, isBroadcast: true);
  }

  void registerNativeCallback() {
    impl._bindings.sqlite3_update_hook(
      impl._handle,
      _updateCallbackPtr.cast(),
      Pointer.fromAddress(id),
    );
  }

  void unregisterNativeCallback() {
    impl._bindings.sqlite3_update_hook(
      impl._handle,
      nullPtr(),
      Pointer.fromAddress(id),
    );
  }

  void addCallback(void Function(SqliteUpdate) callback) {
    final isFirstListener = listeners.isEmpty && callbacks.isEmpty;
    callbacks.add(callback);

    if (isFirstListener) {
      _listeners[id] = listeners;
      _callbacks[id] = callbacks;
      registerNativeCallback();
    }
  }

  void removeCallback(void Function(SqliteUpdate) callback) {
    callbacks.remove(callback);

    if (listeners.isEmpty && callbacks.isEmpty && !closed) {
      unregisterNativeCallback();
    }
  }

  void addListener(MultiStreamController<SqliteUpdate> listener) {
    final isFirstListener = listeners.isEmpty && callbacks.isEmpty;
    listeners.add(listener);

    if (isFirstListener) {
      _listeners[id] = listeners;
      _callbacks[id] = callbacks;
      registerNativeCallback();
    }
  }

  void removeListener(MultiStreamController<SqliteUpdate> listener) {
    listeners.remove(listener);

    if (listeners.isEmpty && callbacks.isEmpty && !closed) {
      unregisterNativeCallback();
    }
  }

  void close() {
    closed = true;
    for (final listener in listeners) {
      listener.close();
    }

    unregisterNativeCallback();
  }
}
