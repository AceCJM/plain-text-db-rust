use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

// using a type allows us to leverage Serde to handle arbitrary information.
type DbDataType = Vec<u8>;

// deriving will allow us to use any serde implementation.
// custom derive to sidestep the arc/rwlock
#[derive(Serialize, Deserialize)]
struct DB {
    #[serde(with = "arc_rwlock_serde")]
    internal_db_table: Arc<RwLock<HashMap<String, HashMap<String, DbDataType>>>>,
}

#[derive(Debug)]
struct Error {
    message: String,
}
use convert_t_to_db_data_type::{convert_into_data_type, rebuild_from_data_type};

impl DB {
    pub fn new() -> Self {
        let db_tables = HashMap::new();
        let internal_access_controls = Arc::new(RwLock::new(db_tables));
        return Self {
            internal_db_table: internal_access_controls,
        };
    }

    /// Rebuilds the database from a given byte slice.
    /// 
    /// Will raise an error if the provided slice is invalid.
    pub fn from_slice(slice:&[u8]) -> Result<Self, Error> {
        let internal_db_table = match rmp_serde::from_slice(slice) {
            Ok(val) => val,
            Err(_err) => return Err(Error::new("Could not decode slice."))
        };

        let access_controls = Arc::new(RwLock::new(internal_db_table));
        return Ok(Self {
            internal_db_table:access_controls
        })
    }

    /// Serializes the current state of the DB into a byte array.
    pub async fn to_vec(&self) -> Vec<u8> {
        // if the lock is poisoned we need to unwrap.
        // if the serialization fails we have really bad problems cause it shouldn't.
        return rmp_serde::to_vec(&*self.internal_db_table.read().await).unwrap()
    }

    /// Initializes a new DB table for write use.
    /// If this table already exists, does nothing.
    pub async fn create_new_table(&self, table_name: &str) {
        // read the lock first. if the table already exists, do nothing.
        // this is in a different scope since if it were in the same scope as the write lock acquire the system
        // would deadlock.
        {
            let existent_db_tables = self.internal_db_table.read().await;
            if existent_db_tables.get(table_name).is_some() {
                return;
            }
        }
        let table_contents = HashMap::new();
        let mut write_lock = self.internal_db_table.write().await;
        write_lock.insert(table_name.to_string(), table_contents);
        return;
    }

    // we only support string field names because what sort of monster uses something like a
    // vector for a field name?
    /// Appends data to a table.
    /// If the given data field does not exist, instantiates it.
    /// If the field does exist, overwrites it.
    /// Allows appending any type that implements Serialize.
    pub async fn append_data<T>(
        &self,
        table_name: &str,
        field_name: &str,
        data: &T,
    ) -> Result<(), Error>
    where
        T: Serialize,
    {
        let mut db_tables = self.internal_db_table.write().await;
        if let Some(table) = db_tables.get_mut(table_name) {
            let serialized_value = convert_into_data_type(data);
            table.insert(field_name.to_string(), serialized_value);
        } else {
            return Err(Error::new(
                "Attempted to append data to non-existent table.",
            ));
        }
        Ok(())
    }

    /// Reads a given field from a table into a concrete Rust type.
    /// Will return an error if the table/field requested does not exist.
    pub async fn read_data<T>(&self, table_name: &str, field_name: &str) -> Result<T, Error>
    // some evil lifetime stuff
    // if you try to use a lifetime on the whole function, the return value might outlive self
    // if you tie self to 'a then db_tables gets dropped when we look up the new table since its
    // scope moves into the new layer
    // this basically forces the caller to ensure that T lives long enough.
    where
        T: for<'de> Deserialize<'de>,
    {
        let db_tables = self.internal_db_table.read().await;
        if let Some(table) = db_tables.get(table_name) {
            if let Some(serialized_data) = table.get(field_name) {
                let data_as_type = rebuild_from_data_type::<T>(serialized_data)?;
                Ok(data_as_type)
            } else {
                return Err(Error::new("Requested Table entry does not exist."));
            }
        } else {
            return Err(Error::new("Requested Table does not exist."));
        }
    }

    /// Removes a data entry from a given table.
    /// If that entry never existed, or if the table doesn't exist, does nothing.
    pub async fn remove_data_entry(&self, table_name: &String, field_name: &String) {
        let mut db_tables = self.internal_db_table.write().await;
        if let Some(table) = db_tables.get_mut(table_name) {
            table.remove(field_name);
        }
    }

    /// Removes an entire table from the database.
    /// If that table was never present, does nothing.
    pub async fn remove_table(&self, table_name: &str) {
        let mut db_tables = self.internal_db_table.write().await;
        db_tables.remove(table_name);
    }
}

impl Display for DB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.internal_db_table)
    }
}

impl Error {
    pub fn new(message: &str) -> Self {
        return Self {
            message: message.into(),
        };
    }
}

// helper functions
mod arc_rwlock_serde {
    use serde::de::Deserializer;
    use serde::ser::Serializer;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    pub fn serialize<S, T>(val: &Arc<RwLock<T>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        T::serialize(&*val.blocking_read(), s)
    }

    pub fn deserialize<'de, D, T>(d: D) -> Result<Arc<RwLock<T>>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Ok(Arc::new(RwLock::new(T::deserialize(d)?)))
    }
}

mod convert_t_to_db_data_type {
    use serde::{Deserialize, Serialize};

    use crate::{DbDataType, Error};

    // again panic here since serialization should never fail.
    pub fn convert_into_data_type<T>(data: &T) -> DbDataType
    where
        T: Serialize,
    {
        let data = match rmp_serde::to_vec(data) {
            Ok(val) => val,
            Err(err) => panic!("Could not serialize Type: {}", err.to_string()),
        };
        data
    }

    // we panic here instead of returning an error since that means that
    // we are trying to store one type as another.
    pub fn rebuild_from_data_type<'de, T>(serialized_data: &'de [u8]) -> Result<T, Error>
    where
        T: Deserialize<'de>,
    {
        let data: T = match rmp_serde::from_slice(&serialized_data) {
            Ok(val) => val,
            Err(err) => {
                return Err(Error::new(&format!(
                    "Could not convert data to requested type: {}",
                    err.to_string()
                )))
            }
        };
        Ok(data)
    }
}

#[tokio::main]
async fn main() {
    // loading/unloading the DB is not shown, but you would call a 
    // Serde library serialize function (I recommend RMPack)
    // and write those bytes to a file.

    let db = DB::new();
    db.create_new_table("test").await;
    db.append_data("test", "name", &"test").await.unwrap();

    db.create_new_table("temp_group").await;
    db.append_data("temp_group", "temp_data", &false)
        .await
        .unwrap();
    db.append_data("temp_group", "temp", &"true_data")
        .await
        .unwrap();

    let data = db
        .read_data::<bool>("temp_group", "temp_data")
        .await
        .unwrap();
    println!("{}", data);

    let data = db.read_data::<String>("temp_group", "temp").await.unwrap();
    println!("{}", data);

    let data = db.read_data::<String>("test", "name").await.unwrap();
    println!("{}", data);

    println!("{}", db)
}
