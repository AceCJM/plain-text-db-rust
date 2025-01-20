use std::collections::HashMap;
use std::fs::read_to_string;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

static TEMPLATE_DB: &str = "\ntemp_group\ntemp,true";

//fn is_instance<T>(_: &T) -> String{
//    return std::any::type_name::<T>().to_owned();
//}

fn create_db(db_name: &str) {
    let db: Vec<&str> = vec![db_name, TEMPLATE_DB];
    let db_str: String = db.into_iter().collect();
    let db_file_name: String = format!("{}.pdbr", db_name);
    let path = Path::new(&db_file_name);
    let display = path.display();
    let mut file: File = match File::create(&path) {
        Err(why) => panic!("couldn't create file {}: {}", display, why),
        Ok(file) => file,
    };

    match file.write_all(db_str.as_bytes()) {
        Err(why) => panic!("couldn't write to file {}: {}", display, why),
        Ok(_) => {}
    }
}

fn write_db(db: HashMap<String,HashMap<String,String>>) {
    let mut db_str = String::new();
    let mut temp_str = String::new();
    for (key, value) in &db {
        if key == "name" {
            if let Some(val) = value.get("name") {
                temp_str = val.to_owned();
                temp_str.push_str("\n");
                temp_str.push_str(&db_str);
                db_str = temp_str;
            } 
        } else {
            temp_str = key.to_owned();
            temp_str.push_str("\n");
            db_str.push_str(&temp_str);
            for (i,k) in value {
                temp_str = i.to_string();
                temp_str.push(',');
                temp_str.push_str(&k); 
                db_str.push_str(&temp_str);
            }
        }
    }
    let db_file_name = if let Some(inner) = &db.get("name") {
        if let Some(value) = inner.get("name") {
            format!("{}.pdbr", value)
        } else {
            "default.pdbr".to_string()
        }
    } else {
        "default.pdbr".to_string()
    };
    let path = Path::new(&db_file_name);
    let display = path.display();
    println!("{}",db_str);
    let mut file: File = match File::create(&path) {
        Err(why) => panic!("couldn't create file {}: {}", display, why),
        Ok(file) => file,
    };

    match file.write_all(db_str.as_bytes()) {
        Err(why) => panic!("couldn't write to file {}: {}", display, why),
        Ok(_) => {}
    }
}

// Test
// temp_group
// temp,true
//
// HashMap: [name:db_name,temp_froup:[temp:true]]
//
fn load_db(db_name: &str) -> HashMap<String, HashMap<String, String>> {
    let mut db: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut name_hash: HashMap<String, String> = HashMap::new();
    name_hash.insert("name".to_string(), db_name.to_string());
    db.insert("name".to_string(), name_hash.clone());

    let file_path: String = format!("{}.pdbr", db_name);
    let contents: String =
        read_to_string(file_path).expect("Should have been able to read the file");
    let mut db_vec: Vec<&str> = contents.lines().collect();
    let mut group: HashMap<String, String> = HashMap::new();
    let mut names = Vec::new();
    let mut j: usize = 0;
    db_vec.remove(0);

    for value in db_vec.clone() {
        j += 1;
        if value.contains(",") {
            let value_vec: Vec<&str> = value.split(",").collect();
            group.insert(value_vec[0].to_string(), value_vec[1].to_string());
        } else {
            names.push(value);
            if group.len() > 0 {
                let index: usize = j - 1;
                db.insert(names[index].to_string(), group.to_owned());
                let mut _group: HashMap<String, String> = HashMap::new();
            }
        }
    }
    let index: usize = j - 2;
    db.insert(names[index].to_string(), group.to_owned());
    return db;
}

fn main() {
    create_db("test");
    let db: HashMap<String,HashMap<String,String>> = load_db("test");
    write_db(db);
}
