require "sqlite3"
require "pp"
db = SQLite3::Database.new "starter.db"
db.execute("PRAGMA foreign_keys=ON")
loop do
  print("SQL=>")
  statement = gets.chomp
  if statement == "quit"
    break
  end
  begin
    pp db.execute statement
  rescue SQLite3::SQLException => error
    puts error.message
  end
end

