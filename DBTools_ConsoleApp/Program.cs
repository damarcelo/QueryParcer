using Microsoft.Data.SqlClient;
using System.Data;
using System.Data.Common;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Reflection.PortableExecutable;
using System.Text.RegularExpressions;


// See https://aka.ms/new-console-template for more information
Console.WriteLine("Program Running");
Console.WriteLine();
Console.WriteLine();


String serverName = "obiwan-20240603.avs.private";
String databaseName = "PPSATier2";
String username = "dennis.marcelo";
String password = "Sy5t3mF@!lur3";
String connectionString = $"Data Source={serverName};Initial Catalog={databaseName};User ID={username};Password={password};Trust Server Certificate=True";
SqlConnection conn = new SqlConnection(connectionString);

//String sqlQuery = "SELECT TOP 10 b.*, br.* FROM Branches b LEFT JOIN BranchReps br on (br.BranchID = b.ID)";
//String sqlQuery = "SELECT DISTINCT TOP 10 br.ID, br.UserName, b.* FROM Branches b LEFT JOIN BranchReps br on (br.BranchID = b.ID)";


#region comments
//Console.WriteLine("Connecting ...");
//conn.Open();

///* Display all information about a the query columns 
// * ---------- ---------- ---------- ---------- ---------- 

//SqlCommand command = new SqlCommand(sqlQuery, conn);
//  using (SqlDataReader reader = command.ExecuteReader())
//  {
//    // Get column names
//    DataTable schemaTable = reader.GetSchemaTable();
//    foreach (DataRow row in schemaTable.Rows)
//    {
//      foreach (DataColumn column in schemaTable.Columns)
//      {
//        Console.Write($"{column.ColumnName}: {row[column]}");
//        Console.WriteLine();
//      }
//      Console.WriteLine();
//      Console.WriteLine();
//    }
//    reader.Close();
//  }

//---------- ---------- ---------- ---------- ---------- */


//  Dictionary<string, List<string>> tablesAndColumns = new Dictionary<string, List<string>>();

//  // Regex pattern to match table names and column names
//  string pattern = @"\b(?:FROM|JOIN)\s+(\w+)\s+(?:AS\s+\w+\s+)?(?:ON\s+\w+\.\w+\s*=\s*\w+\.\w+\s+)?(?:,?(\w+)\.)?(\w+)";
//  Regex regex = new Regex(pattern, RegexOptions.IgnoreCase);

//  // Find matches
//  MatchCollection matches = regex.Matches(sqlQuery);

//  foreach (Match match in matches)
//  {
//    string tableName = match.Groups[1].Value;
//    string alias = match.Groups[2].Value;
//    string columnName = match.Groups[3].Value;

//    if (!string.IsNullOrEmpty(alias))
//      tableName = alias;

//    if (!tablesAndColumns.ContainsKey(tableName))
//    {
//      tablesAndColumns[tableName] = new List<string>();
//    }

//    if (!tablesAndColumns[tableName].Contains(columnName))
//    {
//      tablesAndColumns[tableName].Add(columnName);
//    }
//  }

//  foreach (var table in tablesAndColumns)
//  {
//  Console.WriteLine($"Table: {table.Key}");
//  Console.Write("Columns:");
//  foreach (var column in table.Value)
//    {
//    Console.Write($"{column} ");
//  }
//  Console.WriteLine();
//}

//int counter = 0;
//int iAlias = 0;
//string sTName = String.Empty;
//string sTAlias = String.Empty;
//List<QueryResult> MyData = new List<QueryResult>();

//SqlCommand command = new SqlCommand(sqlQuery, conn);
//using (SqlDataReader rows = command.ExecuteReader())
//{

//  while (rows.Read())
//  {
//    for (int i = 0; i < rows.FieldCount; i++)
//    {
//      if (rows.GetName(i).ToUpper() == "ID")
//      {
//        counter = 0;
//        foreach (var table in tablesAndColumns)
//        {
//          sTName = table.Key;
//          sTAlias = table.Value[0].ToString();
//          if (counter == iAlias) break; else counter++;
//        }
//        iAlias++;
//      }
//      MyData.Add(new QueryResult { TableName = sTName, Alias = sTAlias, ColumnName = rows.GetName(i).ToString(), ColumnValue = rows[i].ToString() });
//      //DataRow rec = rows;
//      Console.WriteLine(rows);


//    }
//    iAlias = 0;
//  }
//  rows.Close();




//}


//foreach (QueryResult item in MyData)
//{
//  Console.Write("Table: " + item.TableName);
//  Console.Write(" Table Alias: " + item.Alias);
//  Console.Write(" Column: " + item.ColumnName);
//  Console.Write(" Value: " + item.ColumnValue);
//  Console.WriteLine();
//}


//Console.WriteLine("Disconnecting ...");
//  conn.Close();
#endregion





GenerateReport gr = new GenerateReport();

gr.ConnectionString = $"Data Source={serverName};Initial Catalog={databaseName};User ID={username};Password={password};Trust Server Certificate=True";
gr.SQLDBConnection = conn;
gr.Query = @"SELECT DISTINCT TOP 10 br.ID, br.UserName, b.*, br.* FROM Branches b LEFT JOIN BranchReps br on (br.BranchID = b.ID)";
//gr.Show();
gr.TestCode();
//gr.TestCode_2();



class QueryResult()
  {
    public string? TableName { get; set; }
    public string? Alias { get; set; }
    public string? ColumnName { get; set; }
    public string? ColumnValue { get; set; }
  }


class GenerateReport()
{
  #region DECLARATIONS
  private SqlConnection _SqlDBConnection = new SqlConnection();
  private String _Query = String.Empty;
  private String _ConnectionString = String.Empty;

  private List<QueryResult> MyData = new List<QueryResult>();
  private Dictionary<string, List<string>> tablesAndColumns = new Dictionary<string, List<string>>();

  #endregion

  #region EVENTS
  #endregion

  #region METHODS  
  public void Show()
  {
    Console.WriteLine("Connecting ...");
    _SqlDBConnection.Open();

    getTableAndColumns();
    processInformation();
    displayContent();

    Console.WriteLine("Disconnecting ...");
    _SqlDBConnection.Close();
  }

  public void TestCode()
  {
    const string regex = @"(?<=(?:FROM|JOIN)[\s(]+)(?>\w+)(?=[\s)]*(?:\s+(?:AS\s+)?\w+)?(?:$|\s+(?:WHERE|ON|(?:LEFT|RIGHT)?\s+(?:(?:OUTER|INNER)\s+)?JOIN)))";

    string query = "SELECT e.last_name, e.department_id, d.department_name FROM employees e LEFT OUTER JOIN department d ON ( e.department_id = d.department_id );";

    MatchCollection matches = Regex.Matches(query, regex, RegexOptions.IgnoreCase);

    foreach (Match match in matches)
    {
      string tableName = match.Value;
      string alias = null;

      int aliasIndex = query.IndexOf(" AS ", match.Index) + 4;
      if (aliasIndex > 0)
      {
        int nextSpace = query.IndexOf(' ', aliasIndex);
        if (nextSpace > 0)
        {
          alias = query.Substring(aliasIndex, nextSpace - aliasIndex).Trim();
        }
      }

      Console.WriteLine($"Table Name: {tableName}, Alias: {alias}");
    }

  }
  public void TestCode_2() 
  {

    using (SqlConnection connection = new SqlConnection(_ConnectionString))
    {
      SqlCommand command = new SqlCommand(_Query, connection);
      connection.Open();

      SqlDataReader reader = command.ExecuteReader();

      // Get column names
      DataTable schemaTable = reader.GetSchemaTable();
      foreach (DataRow row in schemaTable.Rows)
      {
        foreach (DataColumn column in schemaTable.Columns)
        {
          Console.WriteLine($"{column.ColumnName}: {row[column]}");
        }
      }

      reader.Close();
    }

    Console.WriteLine(" ");
    Console.WriteLine(" ");
  }

  private void getTableAndColumns()
  {
    // Regex pattern to match table names and column names
    string pattern = @"\b(?:FROM|JOIN)\s+(\w+)\s+(?:AS\s+\w+\s+)?(?:ON\s+\w+\.\w+\s*=\s*\w+\.\w+\s+)?(?:,?(\w+)\.)?(\w+)";
    Regex regex = new Regex(pattern, RegexOptions.IgnoreCase);

    // Find matches
    MatchCollection matches = regex.Matches(this._Query);

    foreach (Match match in matches)
    {
      string tableName = match.Groups[1].Value;
      string alias = match.Groups[2].Value;
      string columnName = match.Groups[3].Value;

      if (!string.IsNullOrEmpty(alias))
        tableName = alias;

      if (!this.tablesAndColumns.ContainsKey(tableName))
      {
        this.tablesAndColumns[tableName] = new List<string>();
      }

      if (!this.tablesAndColumns[tableName].Contains(columnName))
      {
        this.tablesAndColumns[tableName].Add(columnName);
      }
    }
  }
  private void displayContent() {
    foreach (QueryResult item in MyData)
    {
      Console.Write("Table: " + item.TableName);
      Console.Write(" Table Alias: " + item.Alias);
      Console.Write(" Column: " + item.ColumnName);
      Console.Write(" Value: " + item.ColumnValue);
      Console.WriteLine();
    }
  }
  private void processInformation() 
  {
    int counter = 0;
    int iAlias = 0;
    string sTName = String.Empty;
    string sTAlias = String.Empty;    

    SqlCommand command = new SqlCommand(this._Query, this._SqlDBConnection);
    using (SqlDataReader rows = command.ExecuteReader())
    {

      while (rows.Read())
      {
        for (int i = 0; i < rows.FieldCount; i++)
        {
          if (rows.GetName(i).ToUpper() == "ID")
          {
            counter = 0;
            foreach (var table in tablesAndColumns)
            {
              sTName = table.Key;
              sTAlias = table.Value[0].ToString();
              if (counter == iAlias) break; else counter++;
            }
            iAlias++;
          }
          this.MyData.Add(new QueryResult { TableName = sTName, Alias = sTAlias, ColumnName = rows.GetName(i).ToString(), ColumnValue = rows[i].ToString() });
          
        }
        iAlias = 0;
      }
      rows.Close();

    }
  }
  #endregion

  #region PROPERTIES
  public String ConnectionString
  {
    get { return _ConnectionString; }
    set { _ConnectionString = value; }
  }
  public String Query 
  {
    get { return _Query; }
    set { _Query = value; }
  }
  public SqlConnection SQLDBConnection
  {
    get { return _SqlDBConnection; }
    set { _SqlDBConnection = value; }
  }
  #endregion

}