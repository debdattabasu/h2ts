//! HTTP/2 frame codec tests (RFC 7540 §6) — exercises the public `frames` API.

use h2ts_client::frames::*;

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn round_trip(frame: Frame) -> Frame {
    let bytes = serialize_frame(&frame);
    let frames = FrameDecoder::default().push(&bytes).unwrap();
    assert_eq!(frames.len(), 1);
    frames.into_iter().next().unwrap()
}

#[test]
fn settings_ack_is_a_9_byte_empty_frame() {
    let b = serialize_frame(&Frame::Settings {
        ack: true,
        settings: Settings::default(),
    });
    assert_eq!(to_hex(&b), "000000040100000000");
}

#[test]
fn window_update_encodes_increment() {
    let b = serialize_frame(&Frame::WindowUpdate {
        stream_id: 0,
        window_size_increment: 5,
    });
    assert_eq!(
        to_hex(&b),
        format!("{}{}", "000004080000000000", "00000005")
    );
}

#[test]
fn settings_encodes_parameters_in_order() {
    let b = serialize_frame(&Frame::Settings {
        ack: false,
        settings: Settings {
            enable_push: Some(false),
            initial_window_size: Some(65535),
            ..Default::default()
        },
    });
    // header len=12,type=4,flags=0,stream=0 ; then (id=2 -> 0)(id=4 -> 65535)
    assert_eq!(to_hex(&b), "00000c04000000000000020000000000040000ffff");
}

#[test]
fn data_with_end_stream() {
    let f = round_trip(Frame::Data {
        stream_id: 3,
        data: vec![1, 2, 3, 4],
        end_stream: true,
    });
    assert_eq!(
        f,
        Frame::Data {
            stream_id: 3,
            data: vec![1, 2, 3, 4],
            end_stream: true
        }
    );
}

#[test]
fn headers_with_priority_and_flags() {
    let f = round_trip(Frame::Headers {
        stream_id: 5,
        header_block_fragment: vec![0x82, 0x84],
        end_stream: false,
        end_headers: true,
        priority: Some(Priority {
            stream_dependency: 3,
            weight: 16,
            exclusive: true,
        }),
    });
    assert_eq!(
        f,
        Frame::Headers {
            stream_id: 5,
            header_block_fragment: vec![0x82, 0x84],
            end_stream: false,
            end_headers: true,
            priority: Some(Priority {
                stream_dependency: 3,
                weight: 16,
                exclusive: true
            }),
        }
    );
}

#[test]
fn rst_ping_goaway_window_priority() {
    assert_eq!(
        round_trip(Frame::RstStream {
            stream_id: 7,
            error_code: 8
        }),
        Frame::RstStream {
            stream_id: 7,
            error_code: 8
        }
    );
    assert_eq!(
        round_trip(Frame::Ping {
            ack: true,
            opaque_data: [1, 2, 3, 4, 5, 6, 7, 8]
        }),
        Frame::Ping {
            ack: true,
            opaque_data: [1, 2, 3, 4, 5, 6, 7, 8]
        }
    );
    assert_eq!(
        round_trip(Frame::Goaway {
            last_stream_id: 9,
            error_code: 1,
            debug_data: vec![0x61]
        }),
        Frame::Goaway {
            last_stream_id: 9,
            error_code: 1,
            debug_data: vec![0x61]
        }
    );
    assert_eq!(
        round_trip(Frame::WindowUpdate {
            stream_id: 1,
            window_size_increment: 1000
        }),
        Frame::WindowUpdate {
            stream_id: 1,
            window_size_increment: 1000
        }
    );
    // weight 256 -> wire 255 -> back to 256
    assert_eq!(
        round_trip(Frame::Priority {
            stream_id: 1,
            priority: Priority {
                stream_dependency: 0,
                weight: 256,
                exclusive: false
            },
        }),
        Frame::Priority {
            stream_id: 1,
            priority: Priority {
                stream_dependency: 0,
                weight: 256,
                exclusive: false
            },
        }
    );
}

#[test]
fn settings_round_trips_values_incl_boolean() {
    let settings = Settings {
        header_table_size: Some(4096),
        enable_push: Some(false),
        max_concurrent_streams: Some(100),
        initial_window_size: Some(65535),
        max_frame_size: Some(16384),
        max_header_list_size: None,
    };
    assert_eq!(
        round_trip(Frame::Settings {
            ack: false,
            settings: settings.clone()
        }),
        Frame::Settings {
            ack: false,
            settings
        }
    );
}

#[test]
fn push_promise_round_trips() {
    assert_eq!(
        round_trip(Frame::PushPromise {
            stream_id: 1,
            promised_stream_id: 2,
            header_block_fragment: vec![0x82],
            end_headers: true,
        }),
        Frame::PushPromise {
            stream_id: 1,
            promised_stream_id: 2,
            header_block_fragment: vec![0x82],
            end_headers: true,
        }
    );
}

#[test]
fn strips_data_padding() {
    // payload(5) = padLength=2, data=[9,9], pad=[0,0]; type=0(DATA), flags=PADDED(0x8), stream=1
    let bytes = [0, 0, 5, 0, 0x08, 0, 0, 0, 1, 2, 9, 9, 0, 0];
    let frames = FrameDecoder::default().push(&bytes).unwrap();
    assert_eq!(
        frames,
        vec![Frame::Data {
            stream_id: 1,
            data: vec![9, 9],
            end_stream: false
        }]
    );
}

#[test]
fn reassembles_frames_split_at_every_byte_boundary() {
    let mut wire = Vec::new();
    wire.extend(serialize_frame(&Frame::Settings {
        ack: false,
        settings: Settings {
            initial_window_size: Some(65535),
            ..Default::default()
        },
    }));
    wire.extend(serialize_frame(&Frame::Headers {
        stream_id: 1,
        header_block_fragment: vec![0x82, 0x86, 0x84],
        end_stream: true,
        end_headers: true,
        priority: None,
    }));
    wire.extend(serialize_frame(&Frame::Data {
        stream_id: 1,
        data: vec![1, 2, 3, 4, 5],
        end_stream: true,
    }));

    let mut dec = FrameDecoder::default();
    let mut got = Vec::new();
    for i in 0..wire.len() {
        got.extend(dec.push(&wire[i..i + 1]).unwrap());
    }
    let types: Vec<u8> = got.iter().map(frame_type_of).collect();
    assert_eq!(
        types,
        vec![frame_type::SETTINGS, frame_type::HEADERS, frame_type::DATA]
    );
}

#[test]
fn skips_unknown_frame_types() {
    // type 0x63 unknown, len 1, then a valid PING
    let unknown = [0, 0, 1, 0x63, 0, 0, 0, 0, 0, 0xff];
    let ping = serialize_frame(&Frame::Ping {
        ack: false,
        opaque_data: [0; 8],
    });
    let mut input = unknown.to_vec();
    input.extend(ping);
    let frames = FrameDecoder::default().push(&input).unwrap();
    assert_eq!(
        frames,
        vec![Frame::Ping {
            ack: false,
            opaque_data: [0; 8]
        }]
    );
}

fn frame_type_of(f: &Frame) -> u8 {
    match f {
        Frame::Data { .. } => frame_type::DATA,
        Frame::Headers { .. } => frame_type::HEADERS,
        Frame::Priority { .. } => frame_type::PRIORITY,
        Frame::RstStream { .. } => frame_type::RST_STREAM,
        Frame::Settings { .. } => frame_type::SETTINGS,
        Frame::PushPromise { .. } => frame_type::PUSH_PROMISE,
        Frame::Ping { .. } => frame_type::PING,
        Frame::Goaway { .. } => frame_type::GOAWAY,
        Frame::WindowUpdate { .. } => frame_type::WINDOW_UPDATE,
        Frame::Continuation { .. } => frame_type::CONTINUATION,
    }
}
