/**
 * @file sql5300.cpp - SQL Shell, Schema Storage
 * @authors Bobby Brown & Mohith Sairam K V, Milestone 3
 *          Justin Thoreson & Mason Adsero, Milestone 1 & 2
 * @version Milestone 3
 * @see "Seattle University, CPSC5600, Winter 2023"
 */

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include "db_cxx.h"
#include "ParseTreeToString.h"
#include "SQLParser.h"
#include "SQLExec.h"

using namespace hsql;
using namespace std;
 
DbEnv* _DB_ENV; // Global DB environment
const u_int32_t ENV_FLAGS = DB_CREATE | DB_INIT_MPOOL;
const std::string TEST = "test", QUIT = "quit";

/**
 * Establishes a database environment
 * @param envDir The database environment directory
 * @return Pointer to the database environment
 */
void initalizeDbEnv(string);

/**
 * Runs the SQL shell loop and listens for queries
 */
void runSQLShell();

/**
 * Processes a single SQL query
 * @param sql A SQL query (or queries) to process
 */
void handleSQL(string);

/**
 * Processes SQL statements within a parsed query
 * @param parsedSQL A pointer to a parsed SQL query
 */
void handleStatements(SQLParserResult*);

/**
 * Main
*/
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cout << "USAGE: " << argv[0] << " [db_environment]\n";
        return EXIT_FAILURE;
    }

    initalizeDbEnv(argv[1]);

    runSQLShell();

    return EXIT_SUCCESS;
}

void initalizeDbEnv(string envDir)
{
    cout << "(sql5300: running with database environment at " << envDir << endl;

    _DB_ENV = new DbEnv(0U);
    _DB_ENV->set_message_stream(&cout);
    _DB_ENV->set_error_stream(&cerr);
    
    try {
        _DB_ENV->open(envDir.c_str(), ENV_FLAGS, 0);
    } catch (DbException& e) {
        cerr << "(sql5300: " << e.what() << ")" << endl;
        exit(EXIT_FAILURE);
    }
    
    initialize_schema_tables();
}

void runSQLShell() 
{
    string sql = "";
    
    while (sql != QUIT) 
    {
        cout << "SQL> ";
        getline(cin, sql);
        
        if (sql.length())
            handleSQL(sql);
    }
}

void handleSQL(string sql) 
{
    if (sql == QUIT || !sql.length()) return;

    SQLParserResult* const parsedSQL = SQLParser::parseSQLString(sql);

    if (parsedSQL->isValid())
        handleStatements(parsedSQL);
    else if (sql == TEST)
        cout << "test_heap_storage: " << (test_heap_storage() ? "Passed" : "Failed") << endl;
    else
        cout << "INVALID SQL: " << sql << endl << parsedSQL->errorMsg() << endl;
    delete parsedSQL;
}

void handleStatements(hsql::SQLParserResult* const parsedSQL)
{
    size_t nStatements = parsedSQL->size();

    for (size_t i = 0; i < nStatements; i++)
    {
        const SQLStatement* const statement = parsedSQL->getStatement(i);

        try {
            cout << ParseTreeToString::statement(statement) << endl;
            QueryResult *result = SQLExec::execute(statement);
            cout << *result << endl;
            delete result;
        } catch (SQLExecError &e) {
            cout << "Error: " << e.what() << endl;
        }
        
    }
}
