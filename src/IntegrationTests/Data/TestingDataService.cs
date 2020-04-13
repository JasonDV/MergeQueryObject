using System.Collections.Generic;
using ivaldez.Sql.SqlMergeQueryObject;

namespace ivaldez.Sql.IntegrationTests.Data
{
    public class TestingDataService
    {
        private readonly TestingDatabaseService _testingDatabaseService;
        private readonly MergeQueryObject _mergeQueryObject;

        public TestingDataService(
            TestingDatabaseService testingDatabaseService,
            MergeQueryObject mergeQueryObject)
        {
            _testingDatabaseService = testingDatabaseService;
            _mergeQueryObject = mergeQueryObject;
        }
        
        public IEnumerable<SampleSurrogateKey> GetAllSampleSurrogateKey()
        {
            var sql = @"SELECT * FROM dbo.Sample";

            return _testingDatabaseService.Query<SampleSurrogateKey>(sql);
        }

        public IEnumerable<SampleCompositeKeyDto> GetAllSampleCompositeKeyDto()
        {
            var sql = @"SELECT * FROM dbo.Sample";

            return _testingDatabaseService.Query<SampleCompositeKeyDto>(sql);
        }
        
        public void Merge(
            MergeRequest<SampleSurrogateKey> request)
        {
            _testingDatabaseService.WithConnection(conn =>
            {
                _mergeQueryObject.Merge(conn, request);
            });
        }

        public void Merge(
            MergeRequest<SampleSurrogateKeyWithDerivedColumns> request)
        {
            _testingDatabaseService.WithConnection(conn =>
            {
                _mergeQueryObject.Merge(conn, request);
            });
        }

        public void Merge(
            MergeRequest<SampleCompositeKeyPartialUpdateDto> request)
        {
            _testingDatabaseService.WithConnection(conn =>
            {
                _mergeQueryObject.Merge(conn, request);
            });
        }

        public void Merge(
            MergeRequest<SampleSurrogateKeyDifferentNamePrimaryKeyDto> request)
        {
            _testingDatabaseService.WithConnection(conn =>
            {
                _mergeQueryObject.Merge(conn, request);
            });
        }

        public void Merge(
            MergeRequest<SampleSurrogateKeyDifferentNamesDto> request)
        {
            _testingDatabaseService.WithConnection(conn =>
            {
                _mergeQueryObject.Merge(conn, request);
            });
        }

        public void Merge(
            MergeRequest<SampleCompositeKeyDto> request)
        {
            _testingDatabaseService.WithConnection(conn =>
            {
                _mergeQueryObject.Merge(conn, request);
            });
        }

        public void CreateSingleSurrogateKeyTable()
        {
            var sql = @"
CREATE TABLE dbo.Sample(
    Pk INT IDENTITY(1,1) PRIMARY KEY,
    TextValue nvarchar(200) NULL,
    IntValue int NULL,
    DecimalValue decimal(18,8) NULL
)
";

            _testingDatabaseService.Execute(sql);
        }

        public void CreateCompositeKeyTable()
        {
            var sql = @"
CREATE TABLE [dbo].[Sample](
	[Pk1] [int] NOT NULL,
	[Pk2] [nvarchar](10) NOT NULL,
	[TextValue] [nvarchar](200) NULL,
	[IntValue] [int] NOT NULL,
	[DecimalValue] [decimal](18, 8) NOT NULL,
    [IsDeleted] [bit] DEFAULT(0),
    [IsRemovable] [bit] DEFAULT(0)
 CONSTRAINT [PK_Sample] PRIMARY KEY CLUSTERED 
(
	[Pk1] ASC,
	[Pk2] ASC
))
";

            _testingDatabaseService.Execute(sql);
        }

        public void DropTable()
        {
            try
            {
                var sql = @"
DROP TABLE dbo.Sample;
";

                _testingDatabaseService.Execute(sql);
            }
            catch
            {
                //if this fails it is most likely because the table doesn't exist
            }
        }

        public void Insert(SampleCompositeKeyDto sampleCompositeKeyDto)
        {
            var sql = $@"
INSERT INTO [dbo].[Sample]
(
    [Pk1]
    ,[Pk2]
    ,[TextValue]
    ,[IntValue]
    ,[DecimalValue]
    ,[IsDeleted]
)
VALUES
(
    {sampleCompositeKeyDto.Pk1}
    ,'{sampleCompositeKeyDto.Pk2}'
    ,'{sampleCompositeKeyDto.TextValue}'
    ,{sampleCompositeKeyDto.IntValue}
    ,{sampleCompositeKeyDto.DecimalValue}
    ,{(sampleCompositeKeyDto.IsDeleted ? "1":"0")}
)
";

            _testingDatabaseService.Execute(sql);
        }

        public void Insert(SampleSurrogateKey sampleCompositeKeyDto)
        {
            var sql = $@"
SET IDENTITY_INSERT [dbo].[Sample] ON;
INSERT INTO [dbo].[Sample]
(
    [Pk]
    ,[TextValue]
    ,[IntValue]
    ,[DecimalValue]
)
VALUES
(
    {sampleCompositeKeyDto.Pk}
    ,'{sampleCompositeKeyDto.TextValue}'
    ,{sampleCompositeKeyDto.IntValue}
    ,{sampleCompositeKeyDto.DecimalValue}
);
SET IDENTITY_INSERT [dbo].[Sample] OFF;
";

            _testingDatabaseService.Execute(sql);
        }
    }
}