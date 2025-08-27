using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using UnityEngine;

namespace DevelopersHub.LocalMemoryDatabase
{
    public class Database : MonoBehaviour
    {

        private static Database _instance = null;
        private Dictionary<string, ITable> _tables = new Dictionary<string, ITable>();
        private static readonly string _baseFolder = "Database";
        private static readonly string _extension = "table";
        private static string _path = null;
        private static string _directory = null;

        public enum OrderType { Ascending = 1, Descending = 2 }

        private void Awake()
        {
            if (_instance != null && _instance != this)
            {
                Destroy(gameObject);
                return;
            }
            _instance = this;
            DontDestroyOnLoad(gameObject);
        }

        public static void Initialize()
        {
            if (_instance == null)
            {
                _instance = new GameObject("Database").AddComponent<Database>();
                DontDestroyOnLoad(_instance.gameObject);
            }
        }

        public static void LoadTableInMemory<T>(string tableName) where T : struct
        {
            _ = GetTableAsync<T>(tableName);
        }

        public static Table<T> GetTable<T>(string tableName) where T : struct
        {
            if (_instance == null) { Initialize(); }

            if (_instance._tables.TryGetValue(tableName, out ITable existingTable))
            {
                return (Table<T>)existingTable;
            }

            Table<T> table = new Table<T>(tableName);
            if (!table.IsLoaded)
            {
                // Load synchronously for this method since it's expected to be immediate
                table.LoadDataAsync().Wait();
            }
            _instance._tables[tableName] = table;
            return table;
        }

        public static async Task<Table<T>> GetTableAsync<T>(string tableName) where T : struct
        {
            if (_instance == null) { Initialize(); }

            if (_instance._tables.TryGetValue(tableName, out ITable existingTable))
            {
                return (Table<T>)existingTable;
            }

            Table<T> table = new Table<T>(tableName);
            await table.LoadDataAsync();
            _instance._tables[tableName] = table;
            return table;
        }

        public static async Task<List<T>> SelectAsync<T>(string tableName, Func<T, bool> predicate = null) where T : struct
        {
            Table<T> table = await GetTableAsync<T>(tableName);
            return await table.SelectAsync(predicate);
        }

        public static async Task<List<T>> SelectAsync<T>(string tableName, Func<T, bool> predicate, string orderByProperty, OrderType orderType, int? limit = null, int? limitOffset = null) where T : struct
        {
            Table<T> table = await GetTableAsync<T>(tableName);
            return await table.SelectAsync(predicate, orderByProperty, orderType, limit, limitOffset);
        }

        public static async Task<T> GetByPrimaryKey<T>(string tableName, ulong primaryKey) where T : struct
        {
            Table<T> table = await GetTableAsync<T>(tableName);
            return table.GetByPrimaryKey(primaryKey);
        }

        public static ulong Insert<T>(string tableName, T record, bool generatePrimaryKeyIfMissing = true) where T : struct
        {
            Table<T> table = GetTable<T>(tableName);
            return table.Insert(record, generatePrimaryKeyIfMissing);
        }

        public static async Task<ulong> InsertAsync<T>(string tableName, T record, bool generatePrimaryKeyIfMissing = true) where T : struct
        {
            Table<T> table = await GetTableAsync<T>(tableName);
            return await Task.Run(() => table.InsertAsync(record, generatePrimaryKeyIfMissing));
        }

        public static int Update<T>(string tableName, Func<T, bool> predicate, Action<T> updateAction) where T : struct
        {
            Table<T> table = GetTable<T>(tableName);
            return table.Update(predicate, updateAction);
        }

        public static async Task<int> UpdateAsync<T>(string tableName, Func<T, bool> predicate, Action<T> updateAction) where T : struct
        {
            Table<T> table = await GetTableAsync<T>(tableName);
            return await Task.Run(() => table.UpdateAsync(predicate, updateAction));
        }

        public static int Delete<T>(string tableName, Func<T, bool> predicate) where T : struct
        {
            Table<T> table = GetTable<T>(tableName);
            return table.Delete(predicate);
        }

        public static async Task<int> DeleteAsync<T>(string tableName, Func<T, bool> predicate) where T : struct
        {
            Table<T> table = await GetTableAsync<T>(tableName);
            return await Task.Run(() => table.DeleteAsync(predicate));
        }

        private static string GetBaseDirectory()
        {
            if (string.IsNullOrEmpty(_directory))
            {
                _directory = Application.isEditor ? Application.persistentDataPath : Application.persistentDataPath;
                _directory = Path.Combine(_directory, _baseFolder);
                if (!Directory.Exists(_directory)) { Directory.CreateDirectory(_directory); }
            }
            return _directory;
        }

        public static string GetFilePath(string tableName)
        {
            if (string.IsNullOrEmpty(_path))
            {
                _path = Path.Combine(GetBaseDirectory(), $"{tableName}.{_extension}");
            }
            return _path;
        }

        private void OnApplicationQuit()
        {
            // Emergency save all tables
            foreach (var table in _tables.Values)
            {
                if (table is IDisposable disposableTable)
                {
                    // For tables that implement emergency save
                    if (table.GetType().GetMethod("EmergencySaveAsync") != null)
                    {
                        var task = (Task)table.GetType().GetMethod("EmergencySaveAsync").Invoke(table, null);
                        task.Wait(5000); // Wait up to 5 seconds for emergency save
                    }
                    disposableTable.Dispose();
                }
            }
        }

    }
}