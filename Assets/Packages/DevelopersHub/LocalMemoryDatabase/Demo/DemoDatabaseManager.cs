using UnityEngine;
using DevelopersHub.LocalMemoryDatabase;
using TMPro;
using UnityEngine.UI;
using System;
using System.Text;

public class DemoDatabaseManager : MonoBehaviour
{

    [SerializeField] private TMP_InputField _usernameField = null;
    [SerializeField] private TMP_InputField _levelField = null;
    [SerializeField] private TMP_InputField _scoreField = null;
    [SerializeField] private Button _insertButton = null;

    [SerializeField] private TMP_InputField _selectLimitField = null;
    [SerializeField] private TMP_InputField _selectOffsetField = null;
    [SerializeField] private Button _selectButton = null;

    [SerializeField] private TMP_InputField _batchInsertField = null;
    [SerializeField] private Button _batchInsertButton = null;

    [SerializeField] private TMP_InputField _batchDeleteField = null;
    [SerializeField] private Button _batchDeleteButton = null;

    [SerializeField] private TMP_InputField _batchUpdateField = null;
    [SerializeField] private Button _batchUpdateButton = null;

    [SerializeField] private TextMeshProUGUI _logText = null;

    private static readonly string _tableName = "Accounts";
    private const string _glyphs = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private void Awake()
    {
        Database.Initialize();
        Database.LoadTableInMemory<DemoAccountsTable>(_tableName);
    }

    private void Start()
    {
        _insertButton.onClick.AddListener(InsertAccount);
        _selectButton.onClick.AddListener(SelectAccount);
        _batchInsertButton.onClick.AddListener(BatchInsertAccounts);
        _batchDeleteButton.onClick.AddListener(BatchDeleteAccounts);
        _batchUpdateButton.onClick.AddListener(BatchUpdateAccounts);
    }

    private async void InsertAccount()
    {
        string username = _usernameField.text;
        int level = 0;
        float score = 0;
        int.TryParse(_levelField.text, out level);
        float.TryParse(_scoreField.text, out score);
        if (string.IsNullOrEmpty(username))
        {
            Log("Username is null.", Color.yellow);
            return;
        }

        var users = await Database.SelectAsync<DemoAccountsTable>(_tableName, x => x.username == username);
        if (users.Count > 0)
        {
            Log($"Username '{username}' already exists.", Color.red);
            return;
        }

        var user = new DemoAccountsTable();
        user.username = username;
        user.level = level;
        user.score = score;
        user.login = DateTime.UtcNow;
        var id = await Database.InsertAsync(_tableName, user);

        Log($"User '{username}' with id:{id} inserted to database.", Color.white);

        _usernameField.text = "";
        _levelField.text = "";
        _scoreField.text = "";
    }

    private async void SelectAccount()
    {
        int limit = 0;
        int.TryParse(_selectLimitField.text, out limit);
        int offset = 0;
        int.TryParse(_selectOffsetField.text, out offset);
        var users = await Database.SelectAsync<DemoAccountsTable>(_tableName, null, null, default, limit, offset);
        Log($"Selected {users.Count} users.", Color.white);
        for (int i = 0; i < users.Count; i++)
        {
            var user = users[i];
            // Process
        }
    }

    private async void BatchInsertAccounts()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        int count = 0;
        int.TryParse(_batchInsertField.text, out count);
        if (count <= 0) { return; }

        _batchInsertButton.interactable = false;

        try
        {
            for (int i = 0; i < count; i++)
            {
                var user = new DemoAccountsTable();
                user.username = GenerateRandomString(20);
                user.level = 99999;
                user.score = 3.14f;
                user.login = DateTime.UtcNow;
                await Database.InsertAsync(_tableName, user);
            }
            stopwatch.Stop();
            Log($"Inserted {count} users in {stopwatch.ElapsedMilliseconds} milliseconds.", Color.white);
        }
        catch (Exception ex)
        {
            Debug.LogError($"Insert failed: {ex.Message}");
        }
        finally
        {
            _batchInsertButton.interactable = true;
        }
    }

    private async void BatchDeleteAccounts()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        int count = 0;
        int.TryParse(_batchDeleteField.text, out count);
        if (count <= 0) { return; }

        _batchDeleteButton.interactable = false;

        try
        {
            int deleted = 0;
            var accounts = await Database.SelectAsync<DemoAccountsTable>(_tableName, null, "id", Database.OrderType.Ascending, 1);
            if (accounts.Count > 0)
            {
                var smallestId = accounts[0].id;
                for (int i = 0; i < count; i++)
                {
                    ulong id = smallestId + (ulong)i;
                    deleted += await Database.DeleteAsync<DemoAccountsTable>(_tableName, x => x.id == id);
                }
            }
            stopwatch.Stop();
            Log($"Deleted {deleted} users in {stopwatch.ElapsedMilliseconds} milliseconds.", Color.white);
        }
        catch (Exception ex)
        {
            Debug.LogError($"Delete failed: {ex.Message}");
        }
        finally
        {
            _batchDeleteButton.interactable = true;
        }
    }

    private async void BatchUpdateAccounts()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        int count = 0;
        int.TryParse(_batchUpdateField.text, out count);
        if (count <= 0) { return; }

        _batchUpdateButton.interactable = false;

        try
        {
            // Get the smallest ID account
            var accounts = await Database.SelectAsync<DemoAccountsTable>(_tableName, null, "id", Database.OrderType.Ascending, 1);
            if (accounts.Count > 0)
            {
                int updated = 0;
                var smallestId = accounts[0].id;
                for (int i = 0; i < count; i++)
                {
                    var id = smallestId + (ulong)i;
                    updated += await Database.UpdateAsync<DemoAccountsTable>(_tableName, x => x.id == id, account =>
                    {
                        account.level += 1;
                    });
                }
                stopwatch.Stop();
                Log($"Updated {updated} users in {stopwatch.ElapsedMilliseconds} milliseconds.", Color.white);
            }
            else
            {
                Log("No accounts found to update.", Color.yellow);
            }
        }
        catch (Exception ex)
        {
            Debug.LogError($"Update failed: {ex.Message}");
        }
        finally
        {
            _batchUpdateButton.interactable = true;
        }
    }

    private string GenerateRandomString(int length)
    {
        StringBuilder randomString = new StringBuilder();
        for (int i = 0; i < length; i++) { randomString.Append(_glyphs[UnityEngine.Random.Range(0, _glyphs.Length)]); }
        return randomString.ToString();
    }

    private void Log(string log, Color color)
    {
        _logText.text = log;
        _logText.color = color;
    }

}