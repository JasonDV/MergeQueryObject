using System.Collections.Generic;
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
        public void ShouldExecuteWithDefinedTempTableType(bool useRealTempTable)
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

            var infoLogger = new List<string>();
            request.InfoLogger = info => infoLogger.Add(info);

            helper.DataService.Merge(request);

            if (useRealTempTable == false)
            {
                infoLogger.Any(x => x.Contains("##MergeObjectTemp")).Should().BeTrue();
            }

            if (useRealTempTable == true)
            {
                infoLogger.Any(x => x.Contains("dbo.MergeObjectTemp")).Should().BeTrue();
            }

            var sourceDtos = helper.DataService.GetAllSampleDtos<SampleSurrogateKey>().ToArray();

            var firstDto = sourceDtos.First(x => x.TextValue == "JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);
        }
    }
}