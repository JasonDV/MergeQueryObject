Merge Query Object
===============
An abstraction for the SQL Merge statement with many options and efficiencies.

The SQL Merge statement updates, inserts, and/or deletes records. It is intended to make it easier to perform batch operations against a single table as a transactional SQL statement. The hard part about crafting a generic Merge statement is that the options can all be mixed and matched. 

This implementation tries to give the developer access to a useful subset of Merge features, with the aim of making efficient data merges into large tables. 

Nuget: https://www.nuget.org/packages/ivaldez.Sql.SqlMergeQueryObject/

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

## Soft deletes

Generally, you want to avoid using the Delete feature of the Merge statement against large target tables, because it doesn't preform very well. There really isn't a very good option for handling deletes. The most acceptable one is probably "soft deletes," where records are marked for delete and cleaned up after the merge operation whenever it becomes convenient. 

The basic way this is handled is by adding a IsDeleted flag to the table and using the "WhenNotMatchedDeleteBehavior" option on MergeRequest.

```csharp
var request = new MergeRequest<SampleSurrogateKeyDifferentNamesDto>
{
    DataToMerge = dtos,
    TargetTableName = "dbo.Sample",
    PrimaryKeyExpression = t => new object[] {t.Pk},
    //Mark for Delete behavior enabled
    WhenNotMatchedDeleteBehavior = DeleteBehavior.MarkIsDelete
};
```
Now that you have records in the Target table that are marked for delete, we need an efficient way to delete those records. For target tables with 10s or 100s of millions of records, an index on a bit field is relatively inefficient. 

Using a filtered index allows for an efficient delete. Basically, by creating a filtered index, you create a lookup table of rows to be deleted. 

```sql
CREATE NONCLUSTERED INDEX [Idx-IsDeleteFilter] ON [dbo].[Sample]
(
	[IsDeleted] ASC
)
WHERE ([IsDeleted]=(1)) --this is the magic

--The database query optimizer will understand how to use
--the filtered index to identify records to delete
--Note: you should batch these deletes.
DELETE FROM [dbo].[Sample] WHERE IsDeleted=1
```