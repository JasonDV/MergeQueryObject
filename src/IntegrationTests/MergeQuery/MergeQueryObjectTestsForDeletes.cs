using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery
{
    public class MergeQueryObjectTestsForDeletes
    {
        [Fact]
        public void ShouldDeleteWhenNotMatched()
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
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleCompositeKeyDto().ToArray();

            sourceDtos.Length.Should().Be(2);

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
        public void ShouldMarkForDeleteWhenNotMatched()
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
                WhenNotMatchedDeleteBehavior = DeleteBehavior.MarkIsDelete
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService.GetAllSampleCompositeKeyDto().ToArray();

            sourceDtos.Length.Should().Be(3);

            var firstDto = sourceDtos.First(x => x.Pk1 == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.TextValue.Should().Be("JJ");
            firstDto.IntValue.Should().Be(100);
            firstDto.DecimalValue.Should().Be(100.99m);
            firstDto.IsDeleted.Should().BeFalse();

            var secondDto = sourceDtos.First(x => x.Pk1 == 2);
            secondDto.Pk2.Should().Be("B");
            secondDto.TextValue.Should().Be("ZZ");
            secondDto.IntValue.Should().Be(999);
            secondDto.DecimalValue.Should().Be(123.45m);
            secondDto.IsDeleted.Should().BeFalse();

            var thirdDto = sourceDtos.First(x => x.Pk1 == 3);
            thirdDto.Pk2.Should().Be("B");
            thirdDto.TextValue.Should().Be("AA");
            thirdDto.IntValue.Should().Be(1);
            thirdDto.DecimalValue.Should().Be(1);
            thirdDto.IsDeleted.Should().BeTrue();
        }

        [Fact]
        public void ShouldMarkForDeleteWhenCustomFieldName()
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
                WhenNotMatchedDeleteBehavior = DeleteBehavior.MarkIsDelete,
                WhenNotMatchedDeleteFieldName = "IsRemovable"
            };

            helper.DataService.Merge(request);

            var sourceDtos = helper.DataService
                .GetAllSampleCompositeKeyDto()
                .ToArray();

            sourceDtos.Length.Should().Be(3);

            var firstDto = sourceDtos.First(x => x.Pk1 == 3);
            firstDto.IsDeleted.Should().BeFalse();
            firstDto.IsRemovable.Should().BeTrue();
        }
    }
}