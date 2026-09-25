use std::sync::Once;

use pyo3::prelude::*;
use rezzy::hamt::state_group_id_from_lthash;
use rezzy::state::LtHash;
use synapse::database::mtxdb_syn::{
    delete_state_hamt_roots_for_room, get_state_hamt_roots_by_state_group_id,
    get_state_hamt_roots_for_room, open_client, put_state_hamt_roots, repack,
};

static PYTHON: Once = Once::new();

fn initialize_python() {
    PYTHON.call_once(Python::initialize);
}

fn test_root_value(room_id: &str) -> Vec<u8> {
    let mut value = Vec::with_capacity(7 + room_id.len() * 2 + 32 + 2048);
    value.extend_from_slice(b"MTHR");
    value.push(1);
    value.extend_from_slice(&(room_id.len() as u16).to_be_bytes());
    value.extend_from_slice(room_id.as_bytes());
    value.extend_from_slice(&(room_id.len() as u16).to_be_bytes());
    value.extend_from_slice(room_id.as_bytes());
    value.extend(std::iter::repeat_n(0x31, 32));
    value.extend(std::iter::repeat_n(0x04, 2048));
    value
}

fn test_state_group_id() -> Vec<u8> {
    let lattice = [u16::from_le_bytes([0x04, 0x04]); 1024];
    state_group_id_from_lthash(&LtHash(lattice)).to_vec()
}

#[test]
fn room_scoped_root_indexes_survive_repack_and_delete() {
    initialize_python();
    let database = tempfile::tempdir().expect("temporary mtxdb directory");
    let database_path = database.path().to_string_lossy().into_owned();
    let namespace = "integration-root-repack";
    let room = "!integration-root-repack:example.org";
    let state_group = 1;
    let state_group_id = test_state_group_id();
    let value = test_root_value(room);

    Python::attach(|py| {
        open_client(py, database_path).expect("open temporary mtxdb");
        put_state_hamt_roots(
            py,
            namespace.to_owned(),
            room.as_bytes().to_vec(),
            vec![(state_group, value.clone())],
        )
        .expect("write root");

        assert_eq!(
            get_state_hamt_roots_for_room(
                py,
                namespace.to_owned(),
                room.as_bytes().to_vec(),
                vec![state_group],
            )
            .expect("operational lookup before repack"),
            vec![Some(value.clone())]
        );
        assert_eq!(
            get_state_hamt_roots_by_state_group_id(
                py,
                namespace.to_owned(),
                room.as_bytes().to_vec(),
                vec![state_group_id.clone()],
            )
            .expect("semantic lookup before repack"),
            vec![Some(value.clone())]
        );

        repack(py).expect("repack temporary database");

        assert_eq!(
            get_state_hamt_roots_for_room(
                py,
                namespace.to_owned(),
                room.as_bytes().to_vec(),
                vec![state_group],
            )
            .expect("operational lookup after repack"),
            vec![Some(value.clone())]
        );
        assert_eq!(
            get_state_hamt_roots_by_state_group_id(
                py,
                namespace.to_owned(),
                room.as_bytes().to_vec(),
                vec![state_group_id.clone()],
            )
            .expect("semantic lookup after repack"),
            vec![Some(value.clone())]
        );

        delete_state_hamt_roots_for_room(
            py,
            namespace.to_owned(),
            room.as_bytes().to_vec(),
            vec![state_group],
        )
        .expect("delete root");
        assert_eq!(
            get_state_hamt_roots_for_room(
                py,
                namespace.to_owned(),
                room.as_bytes().to_vec(),
                vec![state_group],
            )
            .expect("operational lookup after delete"),
            vec![None]
        );
        assert_eq!(
            get_state_hamt_roots_by_state_group_id(
                py,
                namespace.to_owned(),
                room.as_bytes().to_vec(),
                vec![state_group_id],
            )
            .expect("semantic lookup after delete"),
            vec![None]
        );
    });
}
