using System;
using System.Collections.Generic;
using System.Text;

namespace DB338Core
{
    class IntSchColumn
    {
        public List<string> items;

        private string name;

        public IntSchColumn(string newname, string type)
        {
            name = newname;

            items = new List<string>();
        }
        public string Get(int pos)
        {
            return items[pos];
        }

        public void Delete(int pos)
        {
            items.RemoveAt(pos);
        }

        public string Name { get => name; set => name = value; }
    }
}
