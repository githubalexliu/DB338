using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Text;

namespace DB338Core
{
    class IntSchTable
    {

        private string name;

        private List<IntSchColumn> columns;

        public IntSchTable(string initname)
        {
            name = initname;
            columns = new List<IntSchColumn>();
        }

        public string Name { get => name; set => name = value; }


        public string[,] Select(List<string> cols)
        {
            string[,] results = new string[columns[0].items.Count, cols.Count];

            for (int i = 0; i < cols.Count; ++i)
            {
                for (int j = 0; j < columns.Count; ++j)
                {
                    if (cols[i] == columns[j].Name)
                    {
                        for (int z = 0; z < columns[0].items.Count; ++z)
                        {
                            results[z, i] = columns[j].items[z];
                        }
                    }
                }
            }

            return results;
        }

        public bool Project()
        {
            throw new NotImplementedException();
        }

        public void Insert(List<string> cols, List<string> vals)
        {
            for (int i = 0; i < cols.Count; ++i)
            {
                for (int j = 0; j < columns.Count; ++j)
                {
                    if (columns[j].Name == cols[i])
                    {
                        columns[j].items.Add(vals[i]);
                    }
                }
            }
        }

        public bool AddColumn(string name, string type)
        {
            foreach (IntSchColumn col in columns)
            {
                if (col.Name == name)
                {
                    return false;
                }
            }

            columns.Add(new IntSchColumn(name, type));
            return true;
        }

        public void Update(List<string> colNames, List<string> colValues, List<string> whereNames, List<string> whereValues)
        {
            List<int> rows = new List<int>();
            // check which rows match the where conditions
            for (int i = 0; i < whereNames.Count; i++)
            {
                for (int j = 0; j < columns.Count; j++)
                {
                    if (whereNames[i] == columns[j].Name)
                    {
                        for (int k = 0; k < columns[j].items.Count; k++)
                        {
                            if (columns[j].items[k] == whereValues[i])
                            {
                                rows.Add(k);
                            }
                        }
                    }
                }
            }

            // update the values
            for (int i = 0; i < colNames.Count; i++)
            {
                for (int j = 0; j < columns.Count; j++)
                {
                    if (colNames[i] == columns[j].Name)
                    {
                        foreach (int row in rows)
                        {
                            columns[j].items[row] = colValues[i];
                        }
                    }
                }
            }
        }
    }
}
