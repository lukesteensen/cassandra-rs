use std::io;
use std::fmt;
use std::error;

#[derive(Debug)]
pub enum MyError {
    IO(io::Error),
    Protocol(String),
}

impl From<io::Error> for MyError {
    fn from(err: io::Error) -> MyError {
        MyError::IO(err)
    }
}

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MyError::IO(ref err) => write!(f, "IO error: {}", err),
            MyError::Protocol(ref desc) => write!(f, "Protocol error: {}", desc),
        }
    }
}

impl error::Error for MyError {
    fn description(&self) -> &str {
        match *self {
            MyError::IO(ref err) => err.description(),
            MyError::Protocol(ref desc) => desc,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            MyError::IO(ref err) => Some(err),
            MyError::Protocol(_) => None,
        }
    }
}
