using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowTransformationFluentNotationTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowTransformationFluentNotationTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void Linking3Transformations()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("SourceMultipleLinks");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DestinationMultipleLinks");

            DbSource<string[]> source = new DbSource<string[]>("SourceMultipleLinks", SqlConnection);
            DbDestination<string[]> dest = new DbDestination<string[]>("DestinationMultipleLinks", SqlConnection);
            RowTransformation<string[]> trans1 = new RowTransformation<string[]>(row => row);
            RowTransformation<string[]> trans2 = new RowTransformation<string[]>(row => row);
            RowTransformation<string[]> trans3 = new RowTransformation<string[]>(row => row);

            //Act
            var linkTo1 = source.LinkTo(trans1);
            var linkTo2 = linkTo1.source.LinkTo(trans2);
            var linkTo3 = linkTo2.source.LinkTo(trans3);
            var linkTo4 = linkTo3.source.LinkTo(dest);
            using (linkTo1.link)
            using (linkTo2.link)
            using (linkTo3.link)
            using (linkTo4.link)
            {
                Task sourceT = source.ExecuteAsync();
                Task destT = dest.Completion;

                //Assert
                sourceT.Wait();
                destT.Wait();
                dest2Columns.AssertTestData();
            }
        }

        [Fact]
        public void UsingFluentVoidPredicate()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("SourceMultipleLinks");
            source2Columns.InsertTestData();
            source2Columns.InsertTestDataSet2();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DestinationMultipleLinks");

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("SourceMultipleLinks", SqlConnection);
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("DestinationMultipleLinks", SqlConnection);
            RowTransformation<MySimpleRow> trans1 = new RowTransformation<MySimpleRow>(row => row);

            //Act
            var link1 = source.LinkTo(trans1, row => row.Col1 < 4, row => row.Col1 >= 4);
            var link2 = link1.source.LinkTo(dest);
            using (link1.link)
            using (link2.link)
            {
                Task sourceT = source.ExecuteAsync();
                Task destT = dest.Completion;

                //Assert
                sourceT.Wait();
                destT.Wait();
                dest2Columns.AssertTestData();
            }
        }

        public class MyOtherRow
        {
            [ColumnMap("Col1")]
            public int ColA { get; set; }
            [ColumnMap("Col2")]
            public string ColB { get; set; }
        }

        [Fact]
        public void UsingDifferentObjectTypes()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("SourceMultipleLinks");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DestinationMultipleLinks");

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("SourceMultipleLinks", SqlConnection);
            DbDestination<MyOtherRow> dest = new DbDestination<MyOtherRow>("DestinationMultipleLinks", SqlConnection);
            RowTransformation<MySimpleRow, MyOtherRow> trans1 = new RowTransformation<MySimpleRow, MyOtherRow>
                (row =>
                {
                    return new MyOtherRow()
                    {
                        ColA = row.Col1,
                        ColB = row.Col2
                    };
                }
                );

            //Act
            var link1 = source.LinkTo<MyOtherRow>(trans1);
            var link2 = link1.source.LinkTo(dest);
            using (link1.link)
            using (link2.link)
            {
                //Assert
                source.Execute();
                dest.Wait();
                dest2Columns.AssertTestData();
            }
        }
    }
}
