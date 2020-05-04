using System;
using System.Linq;
using FluentAssertions;
using ivaldez.Sql.IntegrationTests.Data;
using ivaldez.Sql.SqlMergeQueryObject;
using Xunit;

namespace ivaldez.Sql.IntegrationTests.MergeQuery.Issues
{
    public class MergeQueryObjectTestsForIssue10
    {
        [Fact]
        public void ShouldAllowForSqlKeyWordsAsFieldNames()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateCompositeKeyTableWithSqlKeyWords();

            var dtos = new[]
            {
                new SampleCompositeKeyWithKeyWordsDto
                {
                    Exec = 1,
                    Pk2 = "A",
                    Drop = new DateTime(2020, 3, 1),
                    From = "jv",
                }
            };

            var request = new MergeRequest<SampleCompositeKeyWithKeyWordsDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Exec, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete
            };

            helper.DataService.Merge(request);

            var sampleDtos = helper
                .DataService
                .GetAllSampleDtos<SampleCompositeKeyWithKeyWordsDto>().ToArray();

            var firstDto = sampleDtos.First(x => x.Exec == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.Drop.Should().Be( new DateTime(2020, 3, 1));
            firstDto.From.Should().Be("jv");
        }

        [Fact]
        public void ShouldAllowForSqlKeyWordsInFilter()
        {
            var helper = new MergeQueryObjectTestHelper();
            helper.DataService.DropTable();
            helper.DataService.CreateCompositeKeyTableWithSqlKeyWords();

            var dtosSetup = new[]
            {
                new SampleCompositeKeyWithKeyWordsDto
                {
                    Exec = 2,
                    Pk2 = "B",
                    Drop = new DateTime(2020, 4, 1),
                    From = "jv",
                }
            };

            var requestSetup = new MergeRequest<SampleCompositeKeyWithKeyWordsDto>
            {
                DataToMerge = dtosSetup,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Exec, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.None
            };

            helper.DataService.Merge(requestSetup);


            var dtos = new[]
            {
                new SampleCompositeKeyWithKeyWordsDto
                {
                    Exec = 1,
                    Pk2 = "A",
                    Drop = new DateTime(2020, 3, 1),
                    From = "jv",
                }
            };
            
            var startFilter = new DateTime(2020, 3, 1);
            var endFilter = new DateTime(2020, 3, 31);

            var request = new MergeRequest<SampleCompositeKeyWithKeyWordsDto>
            {
                DataToMerge = dtos,
                TargetTableName = "dbo.Sample",
                UseRealTempTable = false,
                PrimaryKeyExpression = t => new object[] {t.Exec, t.Pk2},
                KeepPrimaryKeyInInsertStatement = true,
                TargetDataSetFilter = t => t.Drop >= startFilter && t.Drop <= endFilter,
                WhenNotMatchedDeleteBehavior = DeleteBehavior.Delete
            };

            helper.DataService.Merge(request);

            var sampleDtos = helper
                .DataService
                .GetAllSampleDtos<SampleCompositeKeyWithKeyWordsDto>().ToArray();

            sampleDtos.Length.Should().Be(2);

            var firstDto = sampleDtos.First(x => x.Exec == 1);
            firstDto.Pk2.Should().Be("A");
            firstDto.Drop.Should().Be(new DateTime(2020, 3, 1));
            firstDto.From.Should().Be("jv");

            var secondDtoNotDeletedByFilter = sampleDtos.First(x => x.Exec == 2);
            secondDtoNotDeletedByFilter.Pk2.Should().Be("B");
            secondDtoNotDeletedByFilter.Drop.Should().Be( new DateTime(2020, 4, 1));
            secondDtoNotDeletedByFilter.From.Should().Be("jv");
        }
    }
}