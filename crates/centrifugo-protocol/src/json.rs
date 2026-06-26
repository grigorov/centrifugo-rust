//! JSON framing: streaming decode of consecutive `Command`s from one WebSocket
//! text frame (NDJSON or bare-concatenated), and NDJSON encode of `Reply`s
//! (each followed by `\n`, matching the Go encoder).

use crate::{Command, Reply};

/// Stream-decode consecutive JSON `Command`s from one frame. Tolerates newline
/// separation or bare concatenation (matches Go's `json.Decoder` loop).
pub fn decode_commands(frame: &[u8]) -> Result<Vec<Command>, serde_json::Error> {
    let de = serde_json::Deserializer::from_slice(frame);
    let mut out = Vec::new();
    for cmd in de.into_iter::<Command>() {
        out.push(cmd?);
    }
    Ok(out)
}

/// Encode one `Reply` followed by a newline (NDJSON).
pub fn encode_reply(reply: &Reply) -> Result<Vec<u8>, serde_json::Error> {
    let mut buf = serde_json::to_vec(reply)?;
    buf.push(b'\n');
    Ok(buf)
}

/// Encode many `Reply`s into one NDJSON buffer (one WebSocket frame).
pub fn encode_replies(replies: &[Reply]) -> Result<Vec<u8>, serde_json::Error> {
    let mut buf = Vec::new();
    for r in replies {
        serde_json::to_writer(&mut buf, r)?;
        buf.push(b'\n');
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::value::RawValue;

    fn raw(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_string()).unwrap()
    }

    #[test]
    fn decode_multiple_commands_one_frame_newline() {
        let frame = b"{\"id\":1}\n{\"id\":2,\"method\":1}";
        let cmds = decode_commands(frame).unwrap();
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0].id, 1);
        assert_eq!(cmds[1].id, 2);
    }

    #[test]
    fn decode_multiple_commands_one_frame_concatenated() {
        let frame = b"{\"id\":1}{\"id\":2}";
        let cmds = decode_commands(frame).unwrap();
        assert_eq!(cmds.len(), 2);
    }

    #[test]
    fn encode_reply_appends_newline() {
        let r = Reply::ok(1, raw("{}"));
        assert_eq!(encode_reply(&r).unwrap(), b"{\"id\":1,\"result\":{}}\n");
    }

    #[test]
    fn encode_many_concatenates() {
        let a = Reply::ok(1, raw("{}"));
        let b = Reply::ok(2, raw("{}"));
        assert_eq!(
            encode_replies(&[a, b]).unwrap(),
            b"{\"id\":1,\"result\":{}}\n{\"id\":2,\"result\":{}}\n"
        );
    }
}
