using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ConsoleApp1
{
    internal class Program
    {

        private static string DataSetToJson(DataSet ds)
        {
            var results = new List<object>();
            foreach (var table in ds.Tables.Cast<DataTable>())
            {

                var parentRows = new List<Dictionary<string, object>>();
                foreach (var row in table.Rows.Cast<DataRow>())
                {

                    var childRow = new Dictionary<string, object>();
                    foreach (var column in table.Columns.Cast<DataColumn>())
                    {
                        childRow.Add(column.ColumnName, row[column]);
                    }

                    parentRows.Add(childRow);
                }

                results.Add(new
                {
                    name = table.TableName,
                    items = parentRows
                });
            }

            return JsonConvert.SerializeObject(results, Formatting.Indented);
        }

        private static async Task Main(string[] args)
        {
            var sourceFile = Path.Combine(Environment.CurrentDirectory, "test.mdb");
            var connectionString = "Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=" + sourceFile + ";";
            using (var db = new OdbcConnection(connectionString))
            {
                await db.OpenAsync();

                var schemaTable = db.GetSchema("Tables");
                var dataSet = new DataSet();

                Console.WriteLine();
                Console.WriteLine("Tables:");
                Console.WriteLine();

                var timer = new Stopwatch();
                timer.Start();
                for (var i = 0; i < schemaTable.Rows.Count; i++)
                {
                    //only source tables
                    if (schemaTable.Rows[i]["TABLE_TYPE"].ToString() == "TABLE")
                    {
                        var tableName = schemaTable.Rows[i]["TABLE_NAME"].ToString();
                        var sql = "SELECT * FROM [" + tableName + "]";
                        var dataTable = new DataTable(tableName);

                        using (var command = new OdbcCommand(sql, db))
                        {
                            using (var adapter = new OdbcDataAdapter(command))
                            {
                                adapter.Fill(dataTable);
                            }
                        }

                        Console.WriteLine(tableName + "(" + dataTable.Rows.Count + " rows)");
                        dataSet.Tables.Add(dataTable);
                    }
                }
                dataSet.AcceptChanges();
                timer.Stop();

                Console.WriteLine();
                Console.WriteLine("Done!({0:g})", timer.Elapsed);
                Console.WriteLine();

                var jsonResults = DataSetToJson(dataSet);
                var jsonFile = "test-" + DateTime.Now.ToString("yyyyMMddTHHmmss") + ".json";
                await File.WriteAllTextAsync(Path.Combine(Environment.CurrentDirectory, jsonFile), jsonResults, Encoding.UTF8);

                Console.WriteLine("Data loaded in '" + jsonFile + "'");
            }

            Console.ReadLine();
        }
    }
}
