#include <string>
#include <vector>
#include "core/db.h"

#include "my_client.h"

#ifndef YCSB_MyDB_H
#define YCSB_MyDB_H
namespace ycsbc{
class MyDB : public DB {
 public:
  typedef std::pair<std::string, std::string> KVPair;
  static const int kOK = 0;
  static const int kErrorNoData = 1;
  static const int kErrorConflict = 2;
  ///
  /// Initializes any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance globally.
  ///
  ///
  /// Clears any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance globally.
  ///
  ///
  /// Reads a record from the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of field/value pairs for the result.
  /// @return Zero on success, or a non-zero error code on error/record-miss.
  ///
   int Read(const std::string &table, const std::string &key,
                   const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) override;
  ///
  /// Performs a range scan for a set of records in the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the first record to read.
  /// @param record_count The number of records to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of vector, where each vector contains field/value
  ///        pairs for one record
  /// @return Zero on success, or a non-zero error code on error.
  ///
   int Scan(const std::string &table, const std::string &key,
                   int record_count, const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result) override;
  ///
  /// Updates a record in the database.
  /// Field/value pairs in the specified vector are written to the record,
  /// overwriting any existing values with the same field names.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to write.
  /// @param values A vector of field/value pairs to update in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
   int Update(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) override;
  ///
  /// Inserts a record into the database.
  /// Field/value pairs in the specified vector are written into the record.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to insert.
  /// @param values A vector of field/value pairs to insert in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
   int Insert(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) override;
  ///
  /// Deletes a record from the database.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to delete.
  /// @return Zero on success, a non-zero error code on error.
  ///
   int Delete(const std::string &table, const std::string &key) override;
   MyDB(utils::Properties &props);
   //~MyDB();

  public:

    my_client  *client_object;
    //utils::Timer<double> timer;

};
}
#endif // YCSB_DB_H