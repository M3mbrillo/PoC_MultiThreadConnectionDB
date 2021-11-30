using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Spectre.Console;

namespace Parallel_ChunkQuery
{
    class Program
    {
        static string _user = "sa";
        static string _pw = "hahaha";
        static string _server = "localhost";


        static ConcurrentBag<string> _results = new ();
        static string _connectionString = $"Server={_server};Database=master;User Id={_user};Password={_pw};MultipleActiveResultSets=True";


        static async Task Main(string[] args)
        {
            AnsiConsole.MarkupLine("[bold white on darkblue]POC - Parallel query to same DB[/]");

            using var connection = new SqlConnection(_connectionString);

            var listOfDb = connection.Query<string>
                ("SELECT name, database_id, create_date, * FROM sys.databases WHERE database_id > 4;");


            var chunksDb = listOfDb.Chunk(2);


            var tasks = new List<Task>();
            foreach (var dbs in chunksDb)
            {                               
                tasks.Add(ProcessTask(dbs));
            }

            // await all chunks executes...
            await Task.WhenAll(tasks);

            var table = new Table();
            table.AddColumn(new TableColumn("column1").Centered());
            table.Border(TableBorder.Rounded);
            table.Expand();
            table.Centered();

            foreach (var item in _results)
            {
                table.AddRow(new Markup($"[blue]{item}[/]"));
            }

            AnsiConsole.Write(table);
           
            Console.ReadLine();
        }



        public static async Task ProcessTask(IEnumerable<string> dbs)
        {
            AnsiConsole.MarkupLine("[white on darkred] >> Starting chunk task![/]");

            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            var querys = dbs.Select(dbName => $"SELECT [Value] FROm {dbName}.dbo.Table_demo");

            // OPT 1 -- ONE query with UNION and use ExecuteQuery
            var opt_1_query = string.Join(" union ", querys);
            var task_opt_1_result = connection.QueryAsync<string>(opt_1_query);

            // OPT 2 -- Multiple Query and read one to one with QueryMultipleAsync and MARS Active
            var opt_2_query = string.Join(";", querys);
            var task_opt_2_result = connection.QueryMultipleAsync(opt_2_query);

            // store results in a ConcurrentBag
            var opt_1_result = await task_opt_1_result;
            opt_1_result
                .AsParallel()
                .ForAll(r => _results.Add(r));

            // as a example, of execute the opt 2 read result...
            var opt_2_result = await task_opt_2_result;
            opt_2_result
                .Read<string>()
                .AsParallel()
                .ForAll(r => _results.Add(r));

            connection.Close();
            AnsiConsole.MarkupLine("[darkred on white] << End chunk task![/]");
        }

    }

    public static class IEnumerableExtensions
    {

        /// <summary>
        /// Break a list of items into chunks of a specific size
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int chunksize)
        {
            while (source.Any())
            {
                yield return source.Take(chunksize);
                source = source.Skip(chunksize);
            }
        }
    }
}



