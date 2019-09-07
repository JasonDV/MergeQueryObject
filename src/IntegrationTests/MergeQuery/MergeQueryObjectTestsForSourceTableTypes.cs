using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForSourceTableTypes
    {
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void ShouldExecuteWithTempTable(bool useRealTempTable)
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateSingleSurrogateKeyTable();

            var dtos = new[]
            {
                new SampleSurrogateKey
                {
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                }
            };

            var request = new MergeRequest<SampleSurrogateKey>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = useRealTempTable,
                PrimaryKeyExpression = t => new object[] {t.Pk},
                KeepPrimaryKeyInInsertStatement = false
            };

            helper.DataService.Merge(request);

            var databaseDtos = helper.DataService.GetAllSampleSurrogateKey().ToArray();

            var firstDto = databaseDtos.First(x => x.TextValue == "JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);
        }
    }
}