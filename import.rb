require "sqlite3"
require "csv"

table_list = %w[Customers Categories Employees Shippers Suppliers Products Orders OrderDetails]
foreign_keys = %w[OrderID ProductID CustomerID EmployeeID ShipperID SupplierID CategoryID]

db = SQLite3::Database.new "starter.db"
db.execute("PRAGMA foreign_keys=ON")
table_list.each do |table| 
  begin
    db.execute "DROP TABLE #{table};"
  rescue SQLite3::SQLException
  end
  table_csv = CSV.read("./datafiles/#{table}.csv", headers: true, col_sep: "\t", converters: :numeric)
  table_schema = []
  table_types = []
  table_csv.headers.each_with_index do |header, i|
    if i == 0
      table_schema[0] = "#{header} INTEGER PRIMARY KEY"
      table_types[i] = :integer
    else
      if header == "Price"
        dtype = "FLOAT"
        table_types[i] = :float
      elsif header == "PostalCode"
        dtype = "TEXT"
        table_types[i] = :text
      elsif table_csv[0][i][1].is_a? Integer
        dtype = "INTEGER"
        table_types[i] = :integer
      else
        dtype = "TEXT"
        table_types[i] = :text
      end
      table_schema[i] = "#{header} #{dtype}"
      if foreign_keys.include? header
        table_schema[i] += " NOT NULL"
      end
    end
  end
  table_schema_string = table_schema.join(", ")
  if table == "OrderDetails"
    table_schema_string += ", FOREIGN KEY (OrderID) REFERENCES Orders(OrderID), "\
      "FOREIGN KEY (ProductID) REFERENCES Products(ProductID)"
  elsif table == "Orders" 
    table_schema_string += ", FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID), "\
    "FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID), "\
    "FOREIGN KEY (ShipperID) REFERENCES Shippers(ShipperID)"
  elsif table == "Products"
    table_schema_string += ", FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID), "\
    "FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)"
  end
  # puts table, table_schema_string
  begin
    db.execute("CREATE TABLE #{table} ( #{table_schema_string} );")
  rescue SQLite3::SQLException => e 
    puts "error in create table"
    puts table, table_schema_string
    puts e.message
    raise e 
  end
  table_csv.each do |row|
    values_a = row.fields; 
    values_b = values_a.map.with_index  do | val, i| 
      if table_types[i] == :text
        '"' + val.to_s + '"'
      else
        val
      end
    end
    values = values_b.join(", ");
    # puts "INSERT INTO #{table} VALUES ( #{values} );"
    begin
      db.execute("INSERT INTO #{table} VALUES ( #{values} );")
    rescue SQLite3::SQLException => e 
      puts "an error occurred in insert for #{table} #{values}"
      puts e.messsage
      raise e
    end
  end
end
