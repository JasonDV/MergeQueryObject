using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForTargetDataSetFilters
    {
        [Fact]
        public void ShouldConstrainByFilterClause()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateCompositeKeyTable();

            helper.DataService.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 3,
                Pk2 = "B",
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 2,
                    Pk2 = "B",
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Pk1, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                TargetDataSetFilter = t => t.Pk1 >= 1 && t.Pk1 <= 2,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleDtos<SampleCompositeKeyDto>().ToArray();

            sourceDtos.Length.Should().Be(3);

            var firstDto = sourceDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sourceDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }


        [Fact]
        public void ShouldNotDeleteIfNotInTargetFilter()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateCompositeKeyTable();

            helper.DataService.Insert(new SampleCompositeKeyDto
            {
                Pk1 = 3,
                Pk2 = "B",
                TextValue = "AA",
                IntValue = 1,
                DecimalValue = 1
            });

            var dtos = new[]
            {
                new SampleCompositeKeyDto
                {
                    Pk1 = 1,
                    Pk2 = "A",
                    TextValue = "JJ",
                    IntValue = 100,
                    DecimalValue = 100.99m
                },
                new SampleCompositeKeyDto
                {
                    Pk1 = 2,
                    Pk2 = "B",
                    TextValue = "ZZ",
                    IntValue = 999,
                    DecimalValue = 123.45m
                }
            };

            var request = new MergeRequest<SampleCompositeKeyDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] { t.Pk1, t.Pk2 },
                KeepPrimaryKeyInInsertStatement = true,
                TargetDataSetFilter = t => t.Pk1 >= 1 && t.Pk1 <= 2,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleDtos<SampleCompositeKeyDto>().ToArray();

            sourceDtos.Length.Should().Be(3);

            var firstDto = sourceDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);

            var secondDto = sourceDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
        }
    }
}