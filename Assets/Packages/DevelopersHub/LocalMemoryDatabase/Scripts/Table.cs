using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine;

namespace DevelopersHub.LocalMemoryDatabase
{

    public class Table<T> : ITable where T : struct
    {

        public string Name { get; private set; }
        // private T[] _data;
        private List<T> _dataList = new List<T>();
        private Dictionary<ulong, int> _primaryKeyIndex = null;
        private FieldInfo _primaryKeyField = null;
        private List<FieldInfo> _indexedFields = new List<FieldInfo>();
        private Dictionary<string, object> _indexes = new Dictionary<string, object>();
        private bool _loaded = false; public bool IsLoaded { get { return _loaded; } }
        private bool _loading = false;
        private bool _dirty = false;
        private bool _saving = false;
        private bool _disposed = false;
        private readonly object _loadingLock = new object();
        private readonly object _savingLock = new object();
        private readonly object _dataLock = new object();
        private CancellationTokenSource _saveCancellation = new CancellationTokenSource();
        private ulong _lastPrimaryKey = 0; public ulong LastPrimaryKey { get { return _lastPrimaryKey; } }
        public int Count => _loaded ? _dataList.Count : 0;
        private readonly SemaphoreSlim _loadingSemaphore = new SemaphoreSlim(1, 1);
        private readonly object _primaryKeyLock = new object();
        private readonly ReaderWriterLockSlim _indexLock = new ReaderWriterLockSlim();

        // Cache reflection methods for performance
        private class IndexMethodCache
        {
            public MethodInfo TryGetValue { get; set; }
            public MethodInfo Add { get; set; }
        }

        private Dictionary<Type, IndexMethodCache> _indexMethodCache = new Dictionary<Type, IndexMethodCache>();

        public Table(string name)
        {
            try
            {
                Name = name;
                DiscoverSchema();
            }
            catch (Exception e)
            {
                Debug.LogWarning($"Table constructor failed: {e.Message}");
            }
        }

        private void DiscoverSchema()
        {
            Type type = typeof(T);
            // FieldInfo[] fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);
            FieldInfo[] fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            FieldInfo bestField = null;
            foreach (FieldInfo field in fields)
            {
                if (field.GetCustomAttribute<PrimaryKeyAttribute>() != null)
                {
                    if (bestField != null)
                    {
                        // Debug.LogWarning($"Multiple [PrimaryKey] attributes found in {typeof(T).Name}");
                    }

                    if (bestField == null || (bestField.FieldType != typeof(ulong) && field.FieldType == typeof(ulong)))
                    {
                        bestField = field;
                    }

                    if (field.FieldType != typeof(ulong))
                    {
                        // Debug.LogWarning($"It is recommended for [PrimaryKey] type to be ulong. Found {field.Name} which is {field.FieldType}");
                    }
                }

                if (field.GetCustomAttribute<IndexAttribute>() != null)
                {
                    _indexedFields.Add(field);
                }
            }

            if (bestField != null)
            {
                _primaryKeyField = bestField;
            }
            else
            {
                // Debug.LogWarning($"No [PrimaryKey] field found in {typeof(T).Name}");
            }
        }

        private void BuildIndexes()
        {
            _indexLock.EnterWriteLock();
            try
            {
                // Build primary key index first
                if (_primaryKeyField != null)
                {
                    _primaryKeyIndex = new Dictionary<ulong, int>();
                    _lastPrimaryKey = 0;

                    for (int i = 0; i < _dataList.Count; i++)
                    {
                        ulong keyValue = Convert.ToUInt64(_primaryKeyField.GetValue(_dataList[i]));
                        _primaryKeyIndex[keyValue] = i;
                        if (keyValue > _lastPrimaryKey) _lastPrimaryKey = keyValue;
                    }
                }

                // Build other indexes sequentially (safer)
                foreach (var field in _indexedFields)
                {
                    BuildFieldIndex(field);
                }
            }
            finally
            {
                _indexLock.ExitWriteLock();
            }
        }

        private void BuildFieldIndex(FieldInfo field)
        {
            Type fieldType = field.FieldType;
            Type indexType = typeof(Dictionary<,>).MakeGenericType(fieldType, typeof(List<int>));
            object index = Activator.CreateInstance(indexType);

            for (int i = 0; i < _dataList.Count; i++)
            {
                object keyValue = field.GetValue(_dataList[i]);
                if (keyValue != null)
                {
                    var indicesList = GetOrCreateIndexList(index, keyValue);
                    indicesList.Add(i);
                }
            }

            lock (_indexes)
            {
                _indexes[field.Name] = index;
            }
        }

        private List<int> GetOrCreateIndexList(object index, object key)
        {
            Type indexType = index.GetType();

            if (!_indexMethodCache.TryGetValue(indexType, out IndexMethodCache cache))
            {
                cache = new IndexMethodCache
                {
                    TryGetValue = indexType.GetMethod("TryGetValue"),
                    Add = indexType.GetMethod("Add")
                };
                _indexMethodCache[indexType] = cache;
            }

            object[] parameters = new object[] { key, null };
            bool found = (bool)cache.TryGetValue.Invoke(index, parameters);

            if (found)
            {
                return (List<int>)parameters[1];
            }
            else
            {
                List<int> newList = new List<int>();
                cache.Add.Invoke(index, new object[] { key, newList });
                return newList;
            }
        }

        public T GetByPrimaryKey(ulong primaryKey)
        {
            if (!_loaded || _primaryKeyIndex == null)
                return default;

            // CRITICAL: Thread-safe primary key lookup
            int index;
            lock (_dataLock)
            {
                if (!_primaryKeyIndex.TryGetValue(primaryKey, out index))
                    return default;
            }

            if (index >= 0 && index < _dataList.Count)
            {
                return _dataList[index];
            }

            return default;
        }

        public List<T> GetByIndex<K>(string fieldName, K key)
        {
            if (!_loaded || !_indexes.ContainsKey(fieldName))
            {
                return new List<T>();
            }

            // CRITICAL: Create indices snapshot to avoid race conditions
            int[] indicesSnapshot;
            lock (_dataLock)
            {
                object index = _indexes[fieldName];
                Type indexType = index.GetType();
                MethodInfo tryGetValue = indexType.GetMethod("TryGetValue");
                object[] parameters = new object[] { key, null };

                if ((bool)tryGetValue.Invoke(index, parameters))
                {
                    List<int> indices = (List<int>)parameters[1];
                    indicesSnapshot = indices.ToArray(); // Snapshot of indices
                }
                else
                {
                    return new List<T>();
                }
            }

            // Now safely get the records using the snapshot
            var result = new List<T>();
            foreach (int index in indicesSnapshot)
            {
                if (index >= 0 && index < _dataList.Count)
                {
                    result.Add(_dataList[index]);
                }
            }
            return result;
        }

        public List<T> Select(Func<T, bool> predicate = null)
        {
            if (!_loaded) { return new List<T>(); }
            IEnumerable<T> result = _dataList;
            if (predicate != null)
            {
                result = result.Where(predicate);
            }
            return result.ToList();
        }

        public async Task<List<T>> SelectAsync(Func<T, bool> predicate = null)
        {
            // Wait for loading to complete
            int timeout = 10000; // 10 seconds
            int elapsed = 0;
            while (!_loaded && _loading && elapsed < timeout)
            {
                await Task.Delay(10);
                elapsed += 10;
            }

            if (!_loaded)
            {
                return new List<T>();
            }

            return await Task.Run(() =>
            {
                return Select(predicate);
            });
        }

        public List<T> Select(Func<T, bool> predicate, string orderByProperty, Database.OrderType orderType, int? limit = null, int? limitOffset = null)
        {
            if (!_loaded)
            {
                return new List<T>();
            }

            // CRITICAL: Create a thread-safe snapshot
            List<T> snapshot;
            lock (_dataLock)
            {
                snapshot = new List<T>(_dataList);
            }

            IEnumerable<T> result = snapshot;

            if (predicate != null)
            {
                result = result.Where(predicate);
            }

            // Materialize early to avoid LINQ issues with reflection
            List<T> filteredResults = result.ToList();

            if (!string.IsNullOrEmpty(orderByProperty))
            {
                FieldInfo field = typeof(T).GetField(orderByProperty);
                if (field != null)
                {
                    // Handle common types with typed comparers to avoid boxing
                    if (field.FieldType == typeof(int))
                    {
                        if (orderType == Database.OrderType.Ascending)
                        {
                            filteredResults = filteredResults.OrderBy(x => (int)field.GetValue(x)).ToList();
                        }
                        else
                        {
                            filteredResults = filteredResults.OrderByDescending(x => (int)field.GetValue(x)).ToList();
                        }
                    }
                    else if (field.FieldType == typeof(string))
                    {
                        if (orderType == Database.OrderType.Ascending)
                        {
                            filteredResults = filteredResults.OrderBy(x => (string)field.GetValue(x)).ToList();
                        }
                        else
                        {
                            filteredResults = filteredResults.OrderByDescending(x => (string)field.GetValue(x)).ToList();
                        }
                    }
                    else if (field.FieldType == typeof(ulong))
                    {
                        if (orderType == Database.OrderType.Ascending)
                        {
                            filteredResults = filteredResults.OrderBy(x => (ulong)field.GetValue(x)).ToList();
                        }
                        else
                        {
                            filteredResults = filteredResults.OrderByDescending(x => (ulong)field.GetValue(x)).ToList();
                        }
                    }
                    else
                    {
                        // Fallback for other types
                        var accessor = CreateFieldAccessor<T>(field);
                        if (orderType == Database.OrderType.Ascending)
                        {
                            filteredResults = filteredResults.OrderBy(accessor).ToList();
                        }
                        else
                        {
                            filteredResults = filteredResults.OrderByDescending(accessor).ToList();
                        }
                    }
                }
            }

            // Apply offset and limit
            if (limitOffset.HasValue && limitOffset.Value > 0)
            {
                filteredResults = filteredResults.Skip(limitOffset.Value).ToList();
            }

            if (limit.HasValue)
            {
                filteredResults = filteredResults.Take(limit.Value).ToList();
            }

            return filteredResults;
        }

        public async Task<List<T>> SelectAsync(Func<T, bool> predicate, string orderByProperty, Database.OrderType orderType, int? limit = null, int? limitOffset = null)
        {
            // Wait for loading to complete
            int timeout = 10000; // 10 seconds
            int elapsed = 0;
            while (!_loaded && _loading && elapsed < timeout)
            {
                await Task.Delay(10);
                elapsed += 10;
            }

            if (!_loaded)
            {
                return new List<T>();
            }

            return await Task.Run(() =>
            {
                return Select(predicate, orderByProperty, orderType, limit, limitOffset);
            });
        }

        // Helper method to create fast field access delegate
        private Func<T, object> CreateFieldAccessor<U>(FieldInfo field)
        {
            return item => field.GetValue(item);
        }

        public ulong Insert(T record, bool generatePrimaryKeyIfMissing = true)
        {
            if (!_loaded) { return 0; }

            // CRITICAL: Use a single lock object for the entire operation
            lock (_dataLock)
            {
                T recordToInsert = record;
                ulong primaryKeyValue = 0;

                if (_primaryKeyField != null)
                {
                    primaryKeyValue = Convert.ToUInt64(_primaryKeyField.GetValue(record));

                    // CRITICAL: Check for existing key within the same lock
                    bool keyExists = _primaryKeyIndex != null && _primaryKeyIndex.ContainsKey(primaryKeyValue);

                    if ((primaryKeyValue == 0 || keyExists) && generatePrimaryKeyIfMissing)
                    {
                        // Generate within the same lock context
                        primaryKeyValue = GenerateUniquePrimaryKey();
                        _primaryKeyField.SetValueDirect(__makeref(recordToInsert), primaryKeyValue);
                    }
                    else if (keyExists)
                    {
                        return 0;
                    }
                }
                else
                {
                    return 0;
                }

                AddRecord(recordToInsert);
                return primaryKeyValue;
            }
        }

        public async Task<ulong> InsertAsync(T record, bool generatePrimaryKeyIfMissing = true)
        {
            // Wait for loading to complete
            int timeout = 10000; // 10 seconds
            int elapsed = 0;
            while (!_loaded && _loading && elapsed < timeout)
            {
                await Task.Delay(10);
                elapsed += 10;
            }
            return Insert(record, generatePrimaryKeyIfMissing);
        }

        public int Update(Func<T, bool> predicate, Func<T, T> action)
        {
            if (!_loaded)
            {
                return 0;
            }

            int updatedCount = 0;

            lock (_dataLock)
            {
                for (int i = 0; i < _dataList.Count; i++)
                {
                    if (predicate(_dataList[i]))
                    {
                        // Create a copy to modify
                        T record = _dataList[i];

                        // Apply the update action
                        record = action(record);

                        // Validate primary key uniqueness if exists
                        if (_primaryKeyField != null)
                        {
                            ulong newPrimaryKey = Convert.ToUInt64(_primaryKeyField.GetValue(record));
                            ulong oldPrimaryKey = Convert.ToUInt64(_primaryKeyField.GetValue(_dataList[i]));

                            // If primary key changed, check for uniqueness
                            if (newPrimaryKey != oldPrimaryKey && _primaryKeyIndex.ContainsKey(newPrimaryKey))
                            {
                                Debug.LogWarning($"Duplicate primary key value {newPrimaryKey} in table {Name}");
                                continue;
                            }
                        }
                        //Debug.Log($"Updating record at index {i} in table {Name}");
                        UpdateRecord(i, record);
                        updatedCount++;
                    }
                }
            }

            return updatedCount;
        }

        public async Task<int> UpdateAsync(Func<T, bool> predicate, Func<T, T> action)
        {
            // Wait for loading to complete
            int timeout = 10000; // 10 seconds
            int elapsed = 0;
            while (!_loaded && _loading && elapsed < timeout)
            {
                await Task.Delay(10);
                elapsed += 10;
            }
            return Update(predicate, action);
        }

        public int Delete(Func<T, bool> predicate)
        {
            if (!_loaded)
            {
                return 0;
            }

            int deletedCount = 0;
            List<int> indicesToRemove = new List<int>();

            // First, find all indices to remove
            lock (_dataLock)
            {
                for (int i = 0; i < _dataList.Count; i++)
                {
                    if (predicate(_dataList[i]))
                    {
                        indicesToRemove.Add(i);
                    }
                }

                // Remove records in reverse order to maintain correct indices
                indicesToRemove.Sort((a, b) => b.CompareTo(a));

                foreach (int index in indicesToRemove)
                {
                    RemoveRecord(index);
                    deletedCount++;
                }
            }

            return deletedCount;
        }

        public async Task<int> DeleteAsync(Func<T, bool> predicate)
        {
            // Wait for loading to complete
            int timeout = 5000; // 10 seconds
            int elapsed = 0;
            while (!_loaded && _loading && elapsed < timeout)
            {
                await Task.Delay(10);
                elapsed += 10;
            }
            return Delete(predicate);
        }

        private ulong GenerateUniquePrimaryKey()
        {
            lock (_primaryKeyLock)
            {
                ulong newKey;
                do
                {
                    newKey = _lastPrimaryKey + 1;
                    if (newKey == 0) { newKey = 1; }
                } while (_primaryKeyIndex != null && _primaryKeyIndex.ContainsKey(newKey));
                _lastPrimaryKey = newKey;
                return newKey;
            }
        }

        private void AddRecord(T record)
        {
            lock (_dataLock)
            {
                _dataList.Add(record);
                UpdateIndexesForRecord(_dataList.Count - 1, record);
                MarkDirty();
            }
        }

        private void UpdateRecord(int index, T record)
        {
            if (index < 0 || index >= _dataList.Count) { return; }

            // Remove old record from indexes first
            RemoveRecordFromIndexes(index);

            // Update the record
            _dataList[index] = record;

            // Add updated record to indexes
            UpdateIndexesForRecord(index, record);

            // Mark table as dirty for auto-saving
            MarkDirty();
        }

        private void RemoveRecord(int index)
        {
            if (index < 0 || index >= _dataList.Count) { return; }

            // Remove from indexes first
            RemoveRecordFromIndexes(index);

            // Remove from data list
            _dataList.RemoveAt(index);

            // Update indexes for records that shifted positions
            UpdateIndexesAfterRemoval(index);

            // Mark table as dirty for auto-saving
            MarkDirty();
        }

        private void UpdateIndexesAfterRemoval(int removedIndex)
        {
            _indexLock.EnterWriteLock();
            try
            {
                // Update primary key index for records after the removed index
                if (_primaryKeyField != null && _primaryKeyIndex != null)
                {
                    for (int i = removedIndex; i < _dataList.Count; i++)
                    {
                        ulong keyValue = Convert.ToUInt64(_primaryKeyField.GetValue(_dataList[i]));
                        _primaryKeyIndex[keyValue] = i;
                    }
                }

                // Update other indexes for records after the removed index
                foreach (FieldInfo field in _indexedFields)
                {
                    if (_indexes.TryGetValue(field.Name, out object indexObj))
                    {
                        Type indexType = indexObj.GetType();
                        var values = (System.Collections.IEnumerable)indexType.GetProperty("Keys").GetValue(indexObj);

                        foreach (var key in values)
                        {
                            List<int> indices = GetOrCreateIndexList(indexObj, key);

                            // Update indices that are greater than the removed index
                            for (int i = 0; i < indices.Count; i++)
                            {
                                if (indices[i] > removedIndex)
                                {
                                    indices[i]--;
                                }
                                else if (indices[i] == removedIndex)
                                {
                                    indices.RemoveAt(i);
                                    i--;
                                }
                            }
                        }
                    }
                }
            }
            finally
            {
                _indexLock.ExitWriteLock();
            }
        }

        private void RemoveRecordFromIndexes(int index)
        {
            if (index < 0 || index >= _dataList.Count) return;

            T record = _dataList[index];

            // Remove from primary key index
            if (_primaryKeyField != null && _primaryKeyIndex != null)
            {
                ulong keyValue = Convert.ToUInt64(_primaryKeyField.GetValue(record));
                _primaryKeyIndex.Remove(keyValue);
            }

            // Remove from other indexes
            foreach (FieldInfo field in _indexedFields)
            {
                if (_indexes.TryGetValue(field.Name, out object indexObj))
                {
                    object keyValue = field.GetValue(record);
                    if (keyValue != null)
                    {
                        List<int> indices = GetOrCreateIndexList(indexObj, keyValue);
                        indices.Remove(index);

                        // Remove empty index entries
                        if (indices.Count == 0)
                        {
                            Type indexType = indexObj.GetType();
                            MethodInfo removeMethod = indexType.GetMethod("Remove", new Type[] { field.FieldType });
                            if (removeMethod != null)
                            {
                                removeMethod.Invoke(indexObj, new object[] { keyValue });
                            }
                        }
                    }
                }
            }
        }

        private void RebuildIndexesAfterRemoval(int removedIndex)
        {
            // Update primary key index for records after the removed index
            if (_primaryKeyField != null && _primaryKeyIndex != null)
            {
                for (int i = removedIndex; i < _dataList.Count; i++)
                {
                    ulong keyValue = Convert.ToUInt64(_primaryKeyField.GetValue(_dataList[i]));
                    _primaryKeyIndex[keyValue] = i;
                }
            }

            // Update other indexes for records after the removed index
            foreach (FieldInfo field in _indexedFields)
            {
                if (_indexes.TryGetValue(field.Name, out object indexObj))
                {
                    // Rebuild the index for records after the removed index
                    for (int i = removedIndex; i < _dataList.Count; i++)
                    {
                        object keyValue = field.GetValue(_dataList[i]);
                        if (keyValue != null)
                        {
                            List<int> indices = GetOrCreateIndexList(indexObj, keyValue);

                            // Remove old index if it exists
                            if (indices.Contains(i + 1)) // +1 because we're looking at the position after removal
                            {
                                indices.Remove(i + 1);
                            }

                            // Add new index
                            if (!indices.Contains(i))
                            {
                                indices.Add(i);
                            }
                        }
                    }
                }
            }
        }

        private void UpdateIndexesForRecord(int index, T record)
        {
            _indexLock.EnterWriteLock();
            try
            {
                if (_primaryKeyField != null && _primaryKeyIndex != null)
                {
                    ulong keyValue = Convert.ToUInt64(_primaryKeyField.GetValue(record));
                    _primaryKeyIndex[keyValue] = index;
                }

                foreach (FieldInfo field in _indexedFields)
                {
                    if (_indexes.TryGetValue(field.Name, out object indexObj))
                    {
                        object keyValue = field.GetValue(record);
                        if (keyValue != null)
                        {
                            List<int> indices = GetOrCreateIndexList(indexObj, keyValue);
                            if (!indices.Contains(index))
                            {
                                indices.Add(index);
                            }
                        }
                    }
                }
            }
            finally
            {
                _indexLock.ExitWriteLock();
            }
        }

        private void MarkDirty()
        {
            if (_dirty || _saving) { return; }
            lock (_savingLock)
            {
                _dirty = true;

                // Cancel any pending save and start a new one with delay
                _saveCancellation.Cancel();
                _saveCancellation = new CancellationTokenSource();

                // Auto-save after 1 second delay (debounce multiple changes)
                _ = DelayedSaveAsync(1000, _saveCancellation.Token);
            }
        }

        private async Task DelayedSaveAsync(int delayMs, CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(delayMs, cancellationToken);
                await SaveDataAsync();
            }
            catch (TaskCanceledException)
            {
                // Save was cancelled due to new changes, this is expected
            }
            catch (Exception e)
            {
                Debug.LogWarning($"Delayed save failed: {e.Message}");
            }
        }

        public async Task LoadDataAsync()
        {
            // Early return if already loaded
            if (_loaded) { return; }

            // Use semaphore for proper async synchronization
            await _loadingSemaphore.WaitAsync();
            try
            {
                if (_loaded) { return; }

                // CRITICAL: Replace the problematic timeout logic
                if (_loading)
                {
                    // Use proper async timeout pattern
                    var timeoutTask = Task.Delay(30000);
                    var completionSource = new TaskCompletionSource<bool>();

                    // Check periodically if loading completed
                    _ = Task.Run(async () =>
                    {
                        while (_loading && !timeoutTask.IsCompleted)
                        {
                            await Task.Delay(100);
                        }
                        completionSource.TrySetResult(true);
                    });

                    await Task.WhenAny(completionSource.Task, timeoutTask);
                    if (_loading && timeoutTask.IsCompleted)
                    {
                        Debug.LogError($"Table {Name} loading timed out after 30 seconds");
                        // Continue with empty table rather than throwing exception
                        lock (_dataLock)
                        {
                            _dataList = new List<T>();
                            BuildIndexes();
                            _loaded = true;
                        }
                        return;
                    }
                    if (_loaded) return;
                }

                _loading = true;
                _loaded = false;

                string filePath = Database.GetFilePath(Name);

                // Handle non-existent file case
                if (!File.Exists(filePath))
                {
                    lock (_dataLock)
                    {
                        _dataList = new List<T>();
                        BuildIndexes();
                        _loaded = true;
                        // Debug.Log($"Table '{Name}' file created with empty data.");
                    }
                    return;
                }

                // Load with retry logic for file access conflicts
                const int maxRetries = 3;
                int retryCount = 0;
                bool success = false;

                while (!success && retryCount < maxRetries)
                {
                    try
                    {
                        await Task.Run(() =>
                        {
                            using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                            using (BufferedStream bufferedStream = new BufferedStream(fileStream, 81920)) // 80KB buffer
                            using (BinaryReader reader = new BinaryReader(bufferedStream))
                            {
                                // Read and validate file header
                                if (reader.BaseStream.Length < sizeof(int))
                                {
                                    throw new InvalidDataException($"Table file {Name} is corrupted (too short)");
                                }

                                // Read item count
                                int itemCount = reader.ReadInt32();

                                // Validate reasonable size to prevent memory exhaustion
                                const int maxReasonableSize = 1000000; // 1 million records max
                                if (itemCount < 0 || itemCount > maxReasonableSize)
                                {
                                    throw new InvalidDataException($"Invalid item count {itemCount} in table {Name}");
                                }

                                // Use List<T> instead of array
                                List<T> newDataList = new List<T>(itemCount);
                                var fields = typeof(T).GetFields(BindingFlags.Public | BindingFlags.Instance);

                                // Pre-calculate field readers for faster access
                                var fieldProcessors = fields.Select(f => new
                                {
                                    Field = f,
                                    Reader = GetFieldReader(f.FieldType)
                                }).ToArray();

                                for (int i = 0; i < itemCount; i++)
                                {
                                    try
                                    {
                                        T item = default;

                                        foreach (var processor in fieldProcessors)
                                        {
                                            object value = processor.Reader(reader);
                                            // For structs, we don't need null checks - value types always have values
                                            processor.Field.SetValueDirect(__makeref(item), value);
                                        }

                                        newDataList.Add(item);
                                    }
                                    catch (Exception recordEx)
                                    {
                                        Debug.LogWarning($"Error reading record {i} in table {Name}: {recordEx.Message}");
                                        // Continue with default value for this record
                                        newDataList.Add(default);
                                    }
                                }

                                // Atomic update of data and indexes
                                lock (_dataLock)
                                {
                                    _dataList = newDataList;
                                    BuildIndexes();
                                    _loaded = true;
                                    success = true;
                                }

                                // Debug.Log($"Table '{Name}' loaded successfully with {itemCount} records");
                            }
                        });
                    }
                    catch (IOException ioEx) when (retryCount < maxRetries - 1)
                    {
                        retryCount++;
                        Debug.LogWarning($"Retry {retryCount} for table {Name} due to IO error: {ioEx.Message}");
                        await Task.Delay(100 * retryCount); // Exponential backoff
                    }
                    catch (Exception ex)
                    {
                        Debug.LogError($"Failed to load table {Name}: {ex.Message}");

                        // Fallback: create empty table but don't mark as loaded to prevent data loss
                        lock (_dataLock)
                        {
                            _dataList = new List<T>();
                            _loaded = false; // Don't mark as loaded to force retry later
                        }

                        throw new InvalidOperationException($"Failed to load table {Name} after {retryCount + 1} attempts", ex);
                    }
                }

                if (!success)
                {
                    throw new IOException($"Failed to load table {Name} after {maxRetries} attempts");
                }
            }
            finally
            {
                _loading = false;
                _loadingSemaphore.Release();

                // If loading failed completely, ensure we're in a clean state
                if (!_loaded)
                {
                    lock (_dataLock)
                    {
                        _dataList = new List<T>();
                        _primaryKeyIndex?.Clear();
                        _indexes?.Clear();
                    }
                }
            }
        }

        public void LoadData()
        {
            // Early return if already loaded
            if (_loaded) { return; }

            lock (_loadingLock)
            {
                if (_loaded) { return; }
                if (_loading)
                {
                    // Wait for ongoing loading to complete
                    while (_loading)
                    {
                        System.Threading.Thread.Sleep(10);
                    }
                    if (_loaded) return;
                }

                _loading = true;
                _loaded = false;

                try
                {
                    string filePath = Database.GetFilePath(Name);

                    // Handle non-existent file case
                    if (!File.Exists(filePath))
                    {
                        lock (_dataLock)
                        {
                            _dataList = new List<T>();
                            BuildIndexes();
                            _loaded = true;
                            Debug.Log($"Table '{Name}' file created with empty data.");
                        }
                        return;
                    }

                    // Synchronous file loading with retry logic
                    const int maxRetries = 3;
                    int retryCount = 0;
                    bool success = false;

                    while (!success && retryCount < maxRetries)
                    {
                        try
                        {
                            using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                            using (BufferedStream bufferedStream = new BufferedStream(fileStream, 81920))
                            using (BinaryReader reader = new BinaryReader(bufferedStream))
                            {
                                // Read and validate file header
                                if (reader.BaseStream.Length < sizeof(int))
                                {
                                    throw new InvalidDataException($"Table file {Name} is corrupted (too short)");
                                }

                                // Read item count
                                int itemCount = reader.ReadInt32();

                                // Validate reasonable size
                                const int maxReasonableSize = 1000000;
                                if (itemCount < 0 || itemCount > maxReasonableSize)
                                {
                                    throw new InvalidDataException($"Invalid item count {itemCount} in table {Name}");
                                }

                                List<T> newDataList = new List<T>(itemCount);
                                var fields = typeof(T).GetFields(BindingFlags.Public | BindingFlags.Instance);

                                // Pre-calculate field readers for faster access
                                var fieldProcessors = fields.Select(f => new
                                {
                                    Field = f,
                                    Reader = GetFieldReader(f.FieldType)
                                }).ToArray();

                                for (int i = 0; i < itemCount; i++)
                                {
                                    try
                                    {
                                        T item = default;

                                        foreach (var processor in fieldProcessors)
                                        {
                                            object value = processor.Reader(reader);
                                            processor.Field.SetValueDirect(__makeref(item), value);
                                        }

                                        newDataList.Add(item);
                                    }
                                    catch (Exception recordEx)
                                    {
                                        Debug.LogWarning($"Error reading record {i} in table {Name}: {recordEx.Message}");
                                        newDataList.Add(default);
                                    }
                                }

                                // Atomic update of data and indexes
                                lock (_dataLock)
                                {
                                    _dataList = newDataList;
                                    BuildIndexes();
                                    _loaded = true;
                                    success = true;
                                }

                                //Debug.Log($"Table '{Name}' loaded synchronously with {itemCount} records");
                            }
                        }
                        catch (IOException ioEx) when (retryCount < maxRetries - 1)
                        {
                            retryCount++;
                            Debug.LogWarning($"Retry {retryCount} for table {Name} due to IO error: {ioEx.Message}");
                            System.Threading.Thread.Sleep(100 * retryCount);
                        }
                        catch (Exception ex)
                        {
                            Debug.LogError($"Failed to load table {Name}: {ex.Message}");
                            retryCount++;
                            if (retryCount >= maxRetries)
                            {
                                throw new InvalidOperationException($"Failed to load table {Name} after {maxRetries} attempts", ex);
                            }
                        }
                    }
                }
                finally
                {
                    _loading = false;

                    // If loading failed completely, ensure we're in a clean state
                    if (!_loaded)
                    {
                        lock (_dataLock)
                        {
                            _dataList = new List<T>();
                            _primaryKeyIndex?.Clear();
                            _indexes?.Clear();
                        }
                    }
                }
            }
        }

        // Helper method for field reading with better performance
        private Func<BinaryReader, object> GetFieldReader(Type fieldType)
        {
            if (fieldType == typeof(string)) return reader =>
            {
                bool hasValue = reader.ReadBoolean();
                return hasValue ? reader.ReadString() : null;
            };

            if (fieldType == typeof(int)) return reader => reader.ReadInt32();
            if (fieldType == typeof(uint)) return reader => reader.ReadUInt32();
            if (fieldType == typeof(float)) return reader => reader.ReadSingle();
            if (fieldType == typeof(double)) return reader => reader.ReadDouble();
            if (fieldType == typeof(bool)) return reader => reader.ReadBoolean();
            if (fieldType == typeof(ulong)) return reader => reader.ReadUInt64();
            if (fieldType == typeof(long)) return reader => reader.ReadInt64();
            if (fieldType == typeof(short)) return reader => reader.ReadInt16();
            if (fieldType == typeof(ushort)) return reader => reader.ReadUInt16();
            if (fieldType == typeof(byte)) return reader => reader.ReadByte();
            if (fieldType == typeof(sbyte)) return reader => reader.ReadSByte();
            if (fieldType == typeof(char)) return reader => reader.ReadChar();

            if (fieldType == typeof(DateTime)) return reader => new DateTime(reader.ReadInt64());
            if (fieldType == typeof(TimeSpan)) return reader => new TimeSpan(reader.ReadInt64());
            if (fieldType == typeof(Guid)) return reader => new Guid(reader.ReadBytes(16));
            if (fieldType == typeof(decimal)) return reader => reader.ReadDecimal();

            if (fieldType.IsEnum) return reader => Enum.ToObject(fieldType, reader.ReadInt32());

            // For unsupported types, read and discard the bytes
            return reader =>
            {
                int size = Marshal.SizeOf(fieldType);
                reader.ReadBytes(size);
                return Activator.CreateInstance(fieldType);
            };
        }

        private object GetDefaultValue(Type type)
        {
            return type.IsValueType ? Activator.CreateInstance(type) : null;
        }

        private async Task SaveDataAsync()
        {
            if (!_loaded) return;

            lock (_savingLock)
            {
                if (_saving) return;
                _saving = true;
                _dirty = false;
            }

            string filePath = Database.GetFilePath(Name);
            string tempFilePath = filePath + ".tmp." + Guid.NewGuid().ToString("N"); // Unique temp file

            try
            {
                List<T> dataSnapshot;
                lock (_dataLock)
                {
                    dataSnapshot = new List<T>(_dataList);
                }

                string directory = Path.GetDirectoryName(tempFilePath);
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // CRITICAL: Use atomic save pattern with unique temp file
                await Task.Run(() =>
                {
                    using (var fileStream = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 8192, FileOptions.WriteThrough))
                    using (var writer = new BinaryWriter(fileStream))
                    {
                        writer.Write(dataSnapshot.Count);

                        var fields = typeof(T).GetFields(BindingFlags.Public | BindingFlags.Instance);
                        foreach (T item in dataSnapshot)
                        {
                            foreach (var field in fields)
                            {
                                object value = field.GetValue(item);
                                WriteField(writer, field.FieldType, value);
                            }
                        }
                        writer.Flush();
                        fileStream.Flush(true); // Force flush to disk
                    }
                });

                // CRITICAL: Atomic replace with backup
                if (File.Exists(filePath))
                {
                    string backupPath = filePath + ".bak." + DateTime.Now.Ticks;
                    File.Replace(tempFilePath, filePath, backupPath, true);
                    // Clean up old backups after successful save
                    CleanupOldBackups(filePath);
                }
                else
                {
                    File.Move(tempFilePath, filePath);
                }
            }
            catch (Exception ex)
            {
                Debug.LogError($"Save failed for {Name}: {ex.Message}");
                try { File.Delete(tempFilePath); } catch { }
                // CRITICAL: Re-mark as dirty to retry later
                lock (_savingLock) { _dirty = true; }
            }
            finally
            {
                lock (_savingLock) { _saving = false; }
            }
        }

        private void CleanupOldBackups(string filePath)
        {
            try
            {
                string directory = Path.GetDirectoryName(filePath);
                string pattern = Path.GetFileName(filePath) + ".bak.*";
                var backups = Directory.GetFiles(directory, pattern)
                    .OrderByDescending(f => File.GetCreationTime(f))
                    .Skip(3); // Keep only 3 most recent backups

                foreach (var backup in backups)
                {
                    File.Delete(backup);
                }
            }
            catch { /* Silent cleanup failure */ }
        }

        private void WriteField(BinaryWriter writer, Type fieldType, object value)
        {
            try
            {
                if (fieldType == typeof(string))
                {
                    bool hasValue = value != null;
                    writer.Write(hasValue);
                    if (hasValue) writer.Write((string)value);
                }
                else if (fieldType == typeof(int)) writer.Write((int)value);
                else if (fieldType == typeof(uint)) writer.Write((uint)value);
                else if (fieldType == typeof(float)) writer.Write((float)value);
                else if (fieldType == typeof(double)) writer.Write((double)value);
                else if (fieldType == typeof(bool)) writer.Write((bool)value);
                else if (fieldType == typeof(ulong)) writer.Write((ulong)value);
                else if (fieldType == typeof(long)) writer.Write((long)value);
                else if (fieldType == typeof(short)) writer.Write((short)value);
                else if (fieldType == typeof(ushort)) writer.Write((ushort)value);
                else if (fieldType == typeof(byte)) writer.Write((byte)value);
                else if (fieldType == typeof(sbyte)) writer.Write((sbyte)value);
                else if (fieldType == typeof(char)) writer.Write((char)value);

                else if (fieldType == typeof(DateTime)) writer.Write(((DateTime)value).Ticks);
                else if (fieldType == typeof(TimeSpan)) writer.Write(((TimeSpan)value).Ticks);
                else if (fieldType == typeof(Guid)) writer.Write(((Guid)value).ToByteArray());
                else if (fieldType == typeof(decimal)) writer.Write((decimal)value);

                else if (fieldType.IsEnum) writer.Write(Convert.ToInt32(value));

                else
                {
                    // For unsupported types, write default value
                    Debug.LogWarning($"Unsupported field type for writing: {fieldType.Name}");
                    byte[] bytes = new byte[Marshal.SizeOf(fieldType)];
                    writer.Write(bytes);
                }
            }
            catch (Exception e)
            {
                Debug.LogError($"Error writing field of type {fieldType.Name}: {e.Message}");
                // Write default/empty value for the type
                if (fieldType == typeof(string))
                {
                    writer.Write(false); // null marker
                }
                else
                {
                    byte[] bytes = new byte[Marshal.SizeOf(fieldType)];
                    writer.Write(bytes);
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                // CRITICAL: Cancel any ongoing operations first
                _saveCancellation?.Cancel();

                // Wait for any ongoing saves to complete with proper async pattern
                try
                {
                    int waitTime = 0;
                    while (_saving && waitTime < 5000) // 5 second timeout
                    {
                        System.Threading.Thread.Sleep(100);
                        waitTime += 100;
                    }
                }
                catch { /* Ignore thread interruption */ }

                // Now dispose resources
                try { _saveCancellation?.Dispose(); } catch { }
                _saveCancellation = null;

                try { _loadingSemaphore?.Dispose(); } catch { }

                try { _indexLock?.Dispose(); } catch { }

                // Clear collections safely
                lock (_dataLock)
                {
                    try { _dataList?.Clear(); } catch { }
                    _dataList = null;
                }

                try { _primaryKeyIndex?.Clear(); } catch { }
                _primaryKeyIndex = null;

                lock (_indexes)
                {
                    try { _indexes?.Clear(); } catch { }
                    _indexes = null;
                }

                try { _indexedFields?.Clear(); } catch { }
                _indexedFields = null;

                try { _indexMethodCache?.Clear(); } catch { }
                _indexMethodCache = null;
            }
            _disposed = true;
        }

        // Add this method to the Table class
        public async Task EmergencySaveAsync()
        {
            if (!_loaded || !_dirty) return;

            try
            {
                // Cancel any delayed saves and save immediately
                _saveCancellation?.Cancel();
                if (_dirty)
                {
                    await SaveDataAsync();
                }
            }
            catch (Exception ex)
            {
                Debug.LogError($"Emergency save failed for table {Name}: {ex.Message}");
            }
        }

        ~Table()
        {
            Dispose(false);
        }
    }

    public interface ITable { }

    [AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = false)]
    public sealed class PrimaryKeyAttribute : Attribute { }

    [AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = true)]
    public sealed class IndexAttribute : Attribute { }

}