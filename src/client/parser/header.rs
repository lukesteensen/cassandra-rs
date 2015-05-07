#[derive(Debug, PartialEq, Eq)]
pub struct Header {
    pub version: Version,
    pub flags: Flags,
    pub stream: u16,
    pub opcode: Opcode,
    pub length: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Version {
    Request,
    Response,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Flags {
    pub compression: bool,
    pub tracing: bool,
}

macro_rules! opcodes {
    ( $( $val:expr => $var:ident, )* ) => {
        #[derive(Debug, PartialEq, Eq)]
        pub enum Opcode {
            $(
                $var = $val,
             )*
        }

        pub fn parse_opcode(val: u8) -> Opcode {
            match val {
                $(
                    $val => Opcode::$var,
                 )*
                _ => panic!("unknown opcode: {:02x}", val),
            }
        }
    }
}

opcodes!(
    0x00 => Error,
    0x01 => Startup,
    0x02 => Ready,
    0x03 => Authenticate,
    0x05 => Options,
    0x06 => Supported,
    0x07 => Query,
    0x08 => Result,
    0x09 => Prepare,
    0x0A => Execute,
    0x0B => Register,
    0x0C => Event,
    0x0D => Batch,
    0x0E => AuthChallenge,
    0x0F => AuthResponse,
    0x10 => AuthSuccess,
);
