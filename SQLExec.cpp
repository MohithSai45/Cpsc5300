/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * 
 * @authors Bobby Brown and Mohith Sairam K V
 * @date        Created 2/18/2023
 * @version     Milestone 4
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres)
{
    if (qres.column_names != nullptr)
    {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows)
        {
            for (auto const &column_name: *qres.column_names)
            {
                Value value = row->at(column_name);
                
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                
                out << " ";
            }

            out << endl;
        }
    }

    out << qres.message;
    
    return out;
}

// QueryResult destructor
QueryResult::~QueryResult() 
{
    delete this->column_attributes;
    delete this->column_names;
    delete this->rows;
}

// SQLExec::execute
QueryResult *SQLExec::execute(const SQLStatement *statement) 
{
    // Check if tables have been initialized
    if (SQLExec::tables == nullptr)
    {
        // Create tables if not yet present
        SQLExec::tables = new Tables();
    }

    // Check if indicies have been initiated
    if (SQLExec::indices == nullptr)
    {
        // Create indices if not yet present
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type())
        {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e)
    {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

// Column defintions
void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute)
{
    column_name = col->name;

    // Define column attribute based on data type
    switch (col->type)
    {
        case ColumnDefinition::DataType::INT:
            column_attribute = ColumnAttribute::DataType::INT;
            break;
        case ColumnDefinition::DataType::TEXT:
            column_attribute = ColumnAttribute::DataType::TEXT;
            break;
        default:
            throw SQLExecError("Not supported");
    }
}

// Create
QueryResult *SQLExec::create(const CreateStatement *statement)
{
    // Update _tables schema with new table
    ValueDict row = {{"table_name", Value(statement->tableName)}};
    Handle tableHandler = SQLExec::tables->insert(&row);

    try {
        // Try to make updates to _columns schema
        Handles columnHandler;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        
        try {
            // Traversing through columns to update the list of all colunms and
            // datatypes
            for (ColumnDefinition *column : *statement->columns)
            {
                // Define column values
                Identifier name;
                ColumnAttribute attribute;
                column_definition(column, name, attribute);
                
                // Determine column type
                string type = attribute.get_data_type() == ColumnAttribute::DataType::TEXT ? "TEXT" : "INT";
                
                // Define columns in one row
                row["data_type"] = Value(type);
                row["column_name"] = Value(name);
                
                // Push back row
                columnHandler.push_back(columns.insert(&row));
            }

            // Create table
            DbRelation &table = SQLExec::tables->get_table(statement->tableName);

            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();
        }
        catch (DbRelationError &e) {
            try {
                // Attempt to undo _columns insert
                for (Handle &columnHandle : columnHandler)
                {
                    columns.del(columnHandle);
                }
            }
            catch (DbRelationError &e) {}
            throw;
        }
    }
    catch (DbRelationError &e) {
        try {
            // Attempt to undo _tables insert
            SQLExec::tables->del(tableHandler);
        }
        catch (DbRelationError &e) {}
        throw;
    }

    return new QueryResult("Created new table: " + string(statement->tableName));
}

// DROP
QueryResult *SQLExec::drop(const DropStatement *statement) 
{
    // Verify DropStatement
    if (statement->type != DropStatement::kTable)
        throw SQLExecError("Unrecongized statement");

    // Get table name
    Identifier name = statement->name;

    // Verify that table names are not a schema table
    if (name == Tables::TABLE_NAME || name == Columns::TABLE_NAME)
        throw SQLExecError("Cannot drop a schema table.");

    ValueDict where = {{"table_name", Value(name)}};

    // Remove columns first
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *rows = columns.select(&where);

    for (Handle &row : *rows)
    {
        columns.del(row);
    }
    delete rows;

    // Remove empty table
    DbRelation &table = SQLExec::tables->get_table(name);
    table.drop();

    // Remove table from schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult(nullptr, nullptr, nullptr, "Dropped table " + string(statement->name));
}

// SHOW
QueryResult *SQLExec::show(const ShowStatement *statement)
{
    switch(statement->type)
    {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        default:
            return new QueryResult("not implemented");
    }
}

// SHOW TABLES
QueryResult *SQLExec::show_tables()
{
    // Get column names and attributes
    ColumnNames *names = new ColumnNames();
    ColumnAttributes *attributes = new ColumnAttributes();
    SQLExec::tables->get_columns(Tables::TABLE_NAME, *names, *attributes);

    // Select entries from the table
    Handles *tableResult = SQLExec::tables->select();
    ValueDicts *rows = new ValueDicts();

    // Traverse through the table
    for (Handle &table : *tableResult)
    {
        // Project all entries from column "table_name"
        ValueDict *row = SQLExec::tables->project(table, names);
        Identifier tableName = row->at("table_name").s;

        // Remove _tables and _columns from results
        if (tableName != Tables::TABLE_NAME && tableName != Columns::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }

    delete tableResult;

    return new QueryResult(names, attributes, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

// SHOW COLUMNS
QueryResult *SQLExec::show_columns(const ShowStatement *statement)
{
    // Get column names and attributes
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    ColumnNames *names = new ColumnNames({"table_name", "column_name", "data_type"});
    ColumnAttributes *attributes = new ColumnAttributes({ColumnAttribute(ColumnAttribute::DataType::TEXT)});

    // Select entries from the table
    ValueDict where = {{"table_name", Value(statement->tableName)}};
    Handles *colResult = columns.select(&where);
    ValueDicts *rows = new ValueDicts();

    // Traverse through the table
    for (Handle &row : *colResult)
        rows->push_back(columns.project(row, names));

    delete colResult;

    return new QueryResult(names, attributes, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) { // Set up labels/header
    ColumnNames *colNames = new ColumnNames();
    colNames->push_back("table_name");
    colNames->push_back("index_name");
    colNames->push_back("seq_in_index");
    colNames->push_back("column_name");
    colNames->push_back("index_type");
    colNames->push_back("is_unique");
    ColumnAttributes *colAttr = new ColumnAttributes();
    colAttr->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // Identify index to show
    Identifier tableName = statement->tableName;
    ValueDict where;
    where["table_name"] = Value(tableName);

    // Get rows related to this index from indices schema
    Handles* handles = SQLExec::indices->select(&where);
    unsigned int size = handles->size();
    ValueDicts *rows = new ValueDicts;
    for (auto const &hd : *handles) {
        ValueDict *row = SQLExec::indices->project(hd, colNames);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(colNames, colAttr, rows,
            "successfully returned " + to_string(size) + " rows");
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    //Double check if statement is valid DROP
    if (statement->type != DropStatement::kIndex) {
        return new QueryResult("Unrecognized DROP statement");
    }

    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;
    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
    ValueDict where;
    where["table_name"] = table_name;
    where["index_name"] = index_name;

    Handles* index_handles = SQLExec::indices->select(&where);
    index.drop();

    for (unsigned int i = 0; i < index_handles->size(); i++) {
        SQLExec::indices->del(index_handles->at(i));
    }

    //clear up memory
    delete index_handles;
    return new QueryResult("dropped index " + index_name);
}
