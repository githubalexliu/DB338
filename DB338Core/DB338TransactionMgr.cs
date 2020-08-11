using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB338Core
{
    class DB338TransactionMgr
    {
        //the List of Internal Schema Tables holds the actual data for DB338
        //it is implemented using Lists, which could be replaced.
        List<IntSchTable> tables;

        public DB338TransactionMgr()
        {
            tables = new List<IntSchTable>();
        }

        public string[,] Process(List<string> tokens, string type)
        {
            string[,] results = new string[1,1];
            bool success;

            if (type == "create")
            {
                success = ProcessCreateTableStatement(tokens);
            }
            else if (type == "insert")
            {
                success = ProcessInsertStatement(tokens);
            }
            else if (type == "select")
            {
                results = ProcessSelectStatement(tokens);
            }
            else if (type == "alter")
            {
                results = ProcessAlterStatement(tokens);
            }
            else if (type == "delete")
            {
                results = ProcessDeleteStatement(tokens);
            }
            else if (type == "drop")
            {
                results = ProcessDropStatement(tokens);
            }
            else if (type == "update")
            {
                results = ProcessUpdateStatement(tokens);
            }
            else
            {
                results = null;
            }
            //other parts of SQL to do...

            return results;
        }

        private string[,] ProcessSelectStatement(List<string> tokens)
        {
            // <Select Stm> ::= SELECT <Columns> <From Clause> <Where Clause> <Group Clause> <Having Clause> <Order Clause>

            List<string> colsToSelect = new List<string>();
            int tableOffset = 0;

            for (int i = 1; i < tokens.Count; ++i)
            {
                if (tokens[i] == "from")
                {
                    tableOffset = i + 1;
                    break;
                }
                else if (tokens[i] == ",")
                {
                    continue;
                }
                else
                {
                    colsToSelect.Add(tokens[i]);
                }
            }

            string tableToSelectFrom = tokens[tableOffset];

            for (int i = 0; i < tables.Count; ++i)
            {
                if (tables[i].Name == tableToSelectFrom)
                {
                    return tables[i].Select(colsToSelect);
                }
            }

            return null;
        }

        private bool ProcessInsertStatement(List<string> tokens)
        {
            // <Insert Stm> ::= INSERT INTO Id '(' <ID List> ')' VALUES '(' <Expr List> ')'

            string insertTableName = tokens[2];

            foreach (IntSchTable tbl in tables)
            {
                if (tbl.Name == insertTableName)
                {
                    List<string> columnNames = new List<string>();
                    List<string> columnValues = new List<string>();

                    int offset = 0;

                    for (int i = 4; i < tokens.Count; ++i)
                    {
                        if (tokens[i] == ")")
                        {
                            offset = i + 3;
                            break;
                        }
                        else if (tokens[i] == ",")
                        {
                            continue;
                        }
                        else
                        {
                            columnNames.Add(tokens[i]);
                        }
                    }

                    for (int i = offset; i < tokens.Count; ++i)
                    {
                        if (tokens[i] == ")")
                        {
                            break;
                        }
                        else if (tokens[i] == ",")
                        {
                            continue;
                        }
                        else
                        {
                            columnValues.Add(tokens[i]);
                        }
                    }

                    if (columnNames.Count != columnValues.Count)
                    {
                        return false;
                    }
                    else
                    {
                        tbl.Insert(columnNames, columnValues);
                        return true;
                    }
                }
            }

            return false;
        }

        private bool ProcessCreateTableStatement(List<string> tokens)
        {
            // assuming only the following rule is accepted
            // <Create Stm> ::= CREATE TABLE Id '(' <ID List> ')'  ------ NO SUPPORT for <Constraint Opt>

            string newTableName = tokens[2];

            foreach (IntSchTable tbl in tables)
            {
                if (tbl.Name == newTableName)
                {
                    //cannot create a new table with the same name
                    return false;
                }
            }

            List<string> columnNames = new List<string>();
            List<string> columnTypes = new List<string>();

            int idCount = 2;
            for (int i = 4; i < tokens.Count; ++i)
            {
                if (tokens[i] == ")")
                {
                    break;
                }
                else if (tokens[i] == ",")
                {
                    continue;
                }
                else
                {
                    if (idCount == 2)
                    {
                        columnNames.Add(tokens[i]);
                        --idCount;
                    }
                    else if (idCount == 1)
                    {
                        columnTypes.Add(tokens[i]);
                        idCount = 2;
                    }
                }
            }

            IntSchTable newTable = new IntSchTable(newTableName);

            for (int i = 0; i < columnNames.Count; ++i)
            {
                newTable.AddColumn(columnNames[i], columnTypes[i]);
            }

            tables.Add(newTable);

            return true;
        }

        private string[,] ProcessUpdateStatement(List<string> tokens)
        {
            // <Update Stm> ::= UPDATE Id SET LIST <ID = EXPR> WHERE LIST <ID = EXPR> 
            string updateTableName = tokens[1];
            bool tableFound = false;
            foreach (IntSchTable tbl in tables)
            {
                if (tbl.Name == updateTableName)
                {
                    tableFound = true;
                    List<string> colNames = new List<string>();
                    List<string> colValues = new List<string>();
                    List<string> whereNames = new List<string>();
                    List<string> whereValues = new List<string>();
                    int idCount = 2, i = 3;

                    for (; i < tokens.Count; i++)
                    {
                        if (tokens[i] == "where")
                        {
                            i++;
                            break;
                        }
                        else if (tokens[i] == "," || tokens[i] == "=")
                        {
                            continue;
                        }
                        else
                        {
                            if (idCount == 2)
                            {
                                colNames.Add(tokens[i]);
                                idCount--;
                            }
                            else
                            {
                                colValues.Add(tokens[i]);
                                idCount = 2;
                            }
                        }
                    }

                    for (; i < tokens.Count; i++)
                    {
                        if (tokens[i] == "," || tokens[i] == "=")
                        {
                            continue;
                        }
                        else
                        {
                            if (idCount == 2)
                            {
                                whereNames.Add(tokens[i]);
                                idCount--;
                            }
                            else
                            {
                                whereValues.Add(tokens[i]);
                                idCount = 2;
                            }
                        }
                    }

                    tbl.Update(colNames, colValues, whereNames, whereValues);
                }
            }
            string[,] s = new string[1, 1];
            if (!tableFound)
            {
                s[0, 0] = "Table " + updateTableName + " not found";
            }
            else
            {
                s[0, 0] = "Table " + updateTableName + " updated";
            }
            return s;
        }

        private string[,] ProcessDropStatement(List<string> tokens)
        {
            // <Drop Stm> ::= DROP TABLE Id
            string tableName = tokens[2];
            int dropId = -1;
            for (int i = 0; i < tables.Count; ++i)
            {
                if (tables[i].Name == tableName)
                {
                    dropId = i;
                }
            }
            string[,] s = new string[1, 1];
            if(dropId == -1)
            {
                s[0, 0] = "Could not find table " + tableName;
                return s;
            }
            tables.RemoveAt(dropId);
            s[0, 0] = "Table " + tableName + " dropped";
            return s;
        }

        private string[,] ProcessDeleteStatement(List<string> tokens)
        {
            // <Update Stm> ::= DELETE FROM Id WHERE LIST <ID = EXPR> 
            string deleteTableName = tokens[2];
            bool tableFound = false;
            foreach (IntSchTable tbl in tables)
            {
                if (tbl.Name == deleteTableName)
                {
                    tableFound = true;
                    List<string> whereNames = new List<string>();
                    List<string> whereValues = new List<string>();
                    int idCount = 2;

                    for (int i = 4; i < tokens.Count; i++)
                    {
                        if (tokens[i] == "," || tokens[i] == "=")
                        {
                            continue;
                        }
                        else
                        {
                            if (idCount == 2)
                            {
                                whereNames.Add(tokens[i]);
                                idCount--;
                            }
                            else
                            {
                                whereValues.Add(tokens[i]);
                                idCount = 2;
                            }
                        }
                    }

                    tbl.Delete(whereNames, whereValues);
                }
            }
            string[,] s = new string[1, 1];
            if (!tableFound)
            {
                s[0, 0] = "Table " + deleteTableName + " not found";
            }
            else
            {
                s[0, 0] = "Table " + deleteTableName + " rows deleted";
            }
            return s;
        }

        private string[,] ProcessAlterStatement(List<string> tokens)
        {
            string alterTableName = tokens[2];
            string operation = tokens[3];
            for (int i = 0; i < tables.Count; i++)
            {
                if (alterTableName == tables[i].Name)
                {
                    if (operation == "add")
                    {
                        tables[i].AddColumn(tokens[5], tokens[6]);
                    }
                    else if (operation == "drop")
                    {
                        tables[i].DropColumn(tokens[5]);  
                    }
                    else if (operation == "alter" || operation == "modify")
                    {
                        // not implementing since column type is not used
                    }
                }
            }

            string[,] s = new string[1, 1];
            return s;
        }
    }
}
