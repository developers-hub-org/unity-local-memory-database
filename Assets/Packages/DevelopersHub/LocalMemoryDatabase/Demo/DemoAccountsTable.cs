using System;
using DevelopersHub.LocalMemoryDatabase;

public struct DemoAccountsTable
{
    [PrimaryKey] public ulong id;
    [Index] public string username;
    [Index] public int level;
    public float score;
    public DateTime login;
}
