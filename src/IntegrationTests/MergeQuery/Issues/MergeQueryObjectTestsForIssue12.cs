using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForNullableKey
    {
        [Fact]
        public void ShouldMatchWhenKeyValueHasNullableTypes()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateCompositeKeyTable();

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = null,
                    DecimalValue = 100.99m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 2,
                    Pk2 = "B",
                    TextValue = "ZZ",
                    IntValue = null,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk2, t.TextValue, t.IntValue},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None
            };

            helper.DataService.Merge(request);

            helper.DataService.Merge(request);

            var sampleDtos = helper.DataService.GetAllSampleDtos<SampleCompositeKeyDto>().ToArray();

            var firstDto = sampleDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(null);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sampleDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(null);
            secondDto.DecimalValue.Should().Be(123.45m);
        }
    }
}
