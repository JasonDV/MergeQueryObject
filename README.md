Merge Query Object
===============
An abstraction for the SQL Merge statement with many options and efficiencies.

The SQL Merge statement updates, inserts, and/or deletes records. It is intended to make it easier to perform batch operations against a single table as a transactional SQL statement. The hard part about crafting a generic Merge statement is that the options can all be mixed and matched. 

This implementation tries to give the developer access to a useful subset of Merge features, with the aim of making efficient data merges into large tables. 

## Targets

* .NETFramework 4.6.1
* .NETStandard 2.0

# Basic usage

With this sample table:

```sql
CREATE TABLE dbo.Sample(
    Pk INT IDENTITY(1,1) PRIMARY KEY,
    TextValue nvarchar(200) NULL,
    IntValue int NULL,
    DecimalValue decimal(18,8) NULL
)
```

## Simple Merge

```csharp
var dtos = new[]
{
    new SampleSurrogateKey
    {
        TextValue = "JJ",
        IntValue = 100,
        DecimalValue = 100.99m
    }
};

//The MergeRequest is composed of the options necessary to 
//preform the merge. Not all options are necessary. For the most part
//the default values of options in MergeRequest have no effect.
var request = new MergeRequest<SampleSurrogateKey>
{
    DataToMerge = dtos,
    TargetTableName = "dbo.Sample",
    UseRealTempTable = false,
    PrimaryKeyExpression = t => new object[] {t.Pk},
    KeepPrimaryKeyInInsertStatement = false
};

mergeQueryObject.Merge(conn, request);
```

## Controlling the bulk load operation

```csharp
var dtos = new[]
{
    new SampleSurrogateKeyDifferentNamesDto
    {
        Pk = 100,
        TextValueExtra = "JJ",
        IntValueExtra = 100,
        DecimalValueExtra = 100.99m
    }
};

//A Merge has two associated tables. The first is the target table 
//which will be modified. The second is the temporary table that holds
//values to be "merged" into the target table.
//The temporary table is created using the target table as a template. 
//Below, we can control how the source DTOs get mapped into the 
//temporary table.
var request = new MergeRequest<SampleSurrogateKeyDifferentNamesDto>
{
    DataToMerge = dtos,
    TargetTableName = "dbo.Sample",
    UseRealTempTable = false,
    PrimaryKeyExpression = t => new object[] {t.Pk},
    KeepPrimaryKeyInInsertStatement = false,
    WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete,
    OnMergeUpdateOnly = false,
    BulkLoaderOptions =
        t => t.With(c => c.TextValueExtra, "TextValue")
            .With(c => c.IntValueExtra, "IntValue")
            .With(c => c.DecimalValueExtra, "DecimalValue")
};

mergeQueryObject.Merge(conn, request);
```