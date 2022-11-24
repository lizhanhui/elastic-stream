fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_num_cpu() {
        let logical_core_num = num_cpus::get();
        let physical_core_num = num_cpus::get_physical();
        assert_eq!(true, logical_core_num >= physical_core_num);
    }

    use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
    use std::io::Cursor;

    #[test]
    fn test_byte_order() {
        let mut reader = Cursor::new(vec![2, 5, 3, 0]);
        assert_eq!(Some(517), reader.read_u16::<BigEndian>().ok());
        assert_eq!(Some(768), reader.read_u16::<BigEndian>().ok());

        let mut writer = vec![];
        writer.write_u16::<LittleEndian>(517).unwrap();
        writer.write_u16::<LittleEndian>(768).unwrap();
        assert_eq!(writer, vec![5, 2, 0, 3]);
    }
}
