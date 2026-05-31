// Copyright (C) 2025 Breezy Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

//! The bzrformats error hierarchy, ported from `bzrformats/errors.py`.
//!
//! The base class `BzrFormatsError` subclasses Python's `Exception` and
//! provides the lazy `_fmt % self.__dict__` formatting, bytes coercion, and
//! `__eq__`/`__hash__` semantics that the Python implementation had. Each
//! concrete error is a thin pyo3 subclass that records its keyword/positional
//! attributes on the instance `__dict__` and carries a class-level `_fmt`.
//!
//! Python subclasses (and downstream consumers such as breezy) can still
//! subclass these and override `_fmt`, because `_get_format_string`/`_format`
//! resolve `_fmt` and `internal_error` dynamically off the instance type.

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

/// Recursively convert `bytes` to its `repr()` inside formatting arguments so
/// `fmt % d` and `repr(...)` never produce a bytes string in error messages.
fn coerce_bytes<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let py = value.py();
    if let Ok(b) = value.downcast::<PyBytes>() {
        // repr() produces a b'...' literal, which is reversible.
        return Ok(b.repr()?.into_any());
    }
    if let Ok(t) = value.downcast::<PyTuple>() {
        let items: PyResult<Vec<_>> = t.iter().map(|i| coerce_bytes(&i)).collect();
        return Ok(PyTuple::new(py, items?)?.into_any());
    }
    if let Ok(l) = value.downcast::<PyList>() {
        let items: PyResult<Vec<_>> = l.iter().map(|i| coerce_bytes(&i)).collect();
        return Ok(PyList::new(py, items?)?.into_any());
    }
    Ok(value.clone())
}

/// Base class for errors raised by bzrformats.
///
/// Attributes:
///   internal_error: if True this was probably caused by a brz bug and
///                   should be displayed with a traceback; if False (or
///                   absent) this was probably a user or environment error.
///   _fmt: Format string to display the error; expanded by the instance dict.
#[pyclass(extends = PyException, subclass, dict, module = "bzrformats._bzr_rs.errors")]
pub struct BzrFormatsError;

#[pymethods]
impl BzrFormatsError {
    #[classattr]
    #[allow(non_upper_case_globals)]
    const internal_error: bool = false;

    #[new]
    #[pyo3(signature = (*args, **kwds))]
    fn new(
        args: &Bound<'_, PyTuple>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        // Accept and ignore any positional/keyword arguments, mirroring
        // `Exception.__new__`. This is what lets Python subclasses define their
        // own `__init__(self, a, b, ...)` without the native base `__new__`
        // rejecting the extra positional arguments. Attribute storage happens
        // in `__init__` (for direct construction) or the subclass `__init__`.
        let _ = (args, kwds);
        PyClassInitializer::from(BzrFormatsError)
    }

    #[pyo3(signature = (msg = None, **kwds))]
    fn __init__(
        slf: &Bound<'_, Self>,
        msg: Option<Bound<'_, PyAny>>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        if let Some(msg) = msg {
            if !msg.is_none() {
                slf.setattr("_preformatted_string", msg)?;
                return Ok(());
            }
        }
        slf.setattr("_preformatted_string", slf.py().None())?;
        if let Some(kwds) = kwds {
            for (key, value) in kwds.iter() {
                slf.setattr(key.downcast::<PyString>()?, value)?;
            }
        }
        Ok(())
    }

    fn _format(slf: &Bound<'_, Self>) -> PyResult<String> {
        let py = slf.py();
        let pre = slf
            .getattr("_preformatted_string")
            .ok()
            .filter(|s| !s.is_none());
        if let Some(s) = pre {
            if let Ok(b) = s.downcast::<PyBytes>() {
                return Ok(b.repr()?.extract()?);
            }
            if s.downcast::<PyTuple>().is_ok() {
                return Ok(coerce_bytes(&s)?.repr()?.extract()?);
            }
            return s.str()?.extract();
        }
        let formatted = (|| -> PyResult<Option<String>> {
            let fmt = Self::_get_format_string(slf)?;
            let Some(fmt) = fmt else { return Ok(None) };
            let d = PyDict::new(py);
            let inst_dict = slf.getattr("__dict__")?;
            let inst_dict = inst_dict.downcast::<PyDict>()?;
            for (k, v) in inst_dict.iter() {
                d.set_item(k, coerce_bytes(&v)?)?;
            }
            let result = fmt
                .bind(py)
                .clone()
                .into_any()
                .call_method1("__mod__", (d,))?;
            Ok(Some(result.extract()?))
        })();
        match formatted {
            Ok(Some(s)) => Ok(s),
            Ok(None) => Self::unprintable(slf, py.None().bind(py)),
            Err(e) => Self::unprintable(slf, e.value(py).as_any()),
        }
    }

    fn __str__(slf: &Bound<'_, Self>) -> PyResult<String> {
        Self::_format(slf)
    }

    fn __repr__(slf: &Bound<'_, Self>) -> PyResult<String> {
        let name = slf.get_type().name()?;
        Ok(format!("{}({})", name, Self::_format(slf)?))
    }

    /// Return format string for this exception or None.
    fn _get_format_string(slf: &Bound<'_, Self>) -> PyResult<Option<Py<PyString>>> {
        match slf.getattr("_fmt") {
            Ok(v) if !v.is_none() => Ok(Some(v.downcast::<PyString>()?.clone().unbind())),
            _ => Ok(None),
        }
    }

    fn __eq__(slf: &Bound<'_, Self>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        if !slf.get_type().is(&other.get_type()) {
            return Ok(py.NotImplemented());
        }
        let a = slf.getattr("__dict__")?;
        let b = other.getattr("__dict__")?;
        let equal: bool = a.eq(b)?;
        Ok(pyo3::types::PyBool::new(py, equal)
            .to_owned()
            .into_any()
            .unbind())
    }

    fn __hash__(slf: &Bound<'_, Self>) -> usize {
        slf.as_ptr() as usize
    }
}

/// Provides the `PyClassInitializer` chain rooted at `BzrFormatsError` so a
/// subclass `#[new]` can build the right multi-level layout without repeating
/// every ancestor by hand. Each error implements `init_chain()` by extending
/// its parent's chain with `.add_subclass(Self)`.
trait ErrInit: pyo3::PyClass {
    fn init_chain() -> PyClassInitializer<Self>;
}

impl ErrInit for BzrFormatsError {
    fn init_chain() -> PyClassInitializer<Self> {
        PyClassInitializer::from(BzrFormatsError)
    }
}

/// Emit the unit pyclass and its `ErrInit` chain (shared by both arms). The
/// `#[pymethods]` block (with `_fmt`, `#[new]`, optional `internal_error` and
/// optional `__init__`) must be emitted separately, because pyo3 0.28 forbids
/// two `#[pymethods]` blocks for the same type.
macro_rules! error_struct {
    (
        $(#[$meta:meta])*
        $name:ident : $parent:ident
    ) => {
        $(#[$meta])*
        #[pyclass(extends = $parent, subclass, dict, module = "bzrformats._bzr_rs.errors")]
        pub struct $name;

        impl ErrInit for $name {
            fn init_chain() -> PyClassInitializer<Self> {
                <$parent as ErrInit>::init_chain().add_subclass($name)
            }
        }
    };
}

/// Define a bzrformats error class.
///
/// `$name` is the new pyclass, `$parent` its (Rust) base, `$fmt` the class
/// `_fmt`. When trailing `; $arg, ...` fields are given, a typed `__init__`
/// stores each positional argument verbatim as a same-named attribute. When no
/// fields are given the class inherits `BzrFormatsError.__init__(msg=None,
/// **kwds)`, so it can be constructed with a preformatted message string (which
/// is how downstream code such as breezy raises these). Pass
/// `internal_error = $ie` to override the inherited `internal_error` flag.
macro_rules! simple_error {
    // No-field variant: inherit the base's (msg=None, **kwds) __init__.
    (
        $(#[$meta:meta])*
        $name:ident : $parent:ident, $fmt:expr
        $(, internal_error = $ie:expr)?
    ) => {
        error_struct!($(#[$meta])* $name : $parent);

        #[pymethods]
        impl $name {
            #[classattr]
            fn _fmt() -> &'static str { $fmt }

            $(
                #[classattr]
                #[allow(non_upper_case_globals)]
                const internal_error: bool = $ie;
            )?

            #[new]
            #[pyo3(signature = (*args, **kwds))]
            fn new(
                args: &Bound<'_, PyTuple>,
                kwds: Option<&Bound<'_, PyDict>>,
            ) -> PyClassInitializer<Self> {
                let _ = (args, kwds);
                <$name as ErrInit>::init_chain()
            }
        }
    };

    // With-fields variant: store each positional argument as an attribute.
    (
        $(#[$meta:meta])*
        $name:ident : $parent:ident, $fmt:expr
        $(, internal_error = $ie:expr)?
        ; $($arg:ident),+
    ) => {
        error_struct!($(#[$meta])* $name : $parent);

        #[pymethods]
        impl $name {
            #[classattr]
            fn _fmt() -> &'static str { $fmt }

            $(
                #[classattr]
                #[allow(non_upper_case_globals)]
                const internal_error: bool = $ie;
            )?

            #[new]
            #[pyo3(signature = (*args, **kwds))]
            fn new(
                args: &Bound<'_, PyTuple>,
                kwds: Option<&Bound<'_, PyDict>>,
            ) -> PyClassInitializer<Self> {
                let _ = (args, kwds);
                <$name as ErrInit>::init_chain()
            }

            #[pyo3(signature = ( $($arg),+ ))]
            fn __init__(
                slf: &Bound<'_, Self>,
                $($arg: Bound<'_, PyAny>),+
            ) -> PyResult<()> {
                $( slf.setattr(stringify!($arg), $arg)?; )+
                Ok(())
            }
        }
    };
}

impl BzrFormatsError {
    fn unprintable(slf: &Bound<'_, Self>, err: &Bound<'_, PyAny>) -> PyResult<String> {
        let name = slf.get_type().name()?;
        let dict = slf.getattr("__dict__")?;
        let fmt = slf
            .getattr("_fmt")
            .unwrap_or_else(|_| slf.py().None().into_bound(slf.py()));
        Ok(format!(
            "Unprintable exception {}: dict={}, fmt={}, error={}",
            name,
            dict.repr()?,
            fmt.repr()?,
            err.repr()?,
        ))
    }
}

// Errors whose __init__ stores positional args verbatim.
// Inventory-serialization errors. BadInventoryFormat is the base; serializer
// code raises the subclasses. (Definitions match the live serializer.py, which
// diverged from the stale errors.py copies before the port.)
simple_error!(BadInventoryFormat: BzrFormatsError, "Root class for inventory serialization errors");
simple_error!(UnsupportedInventoryKind: BzrFormatsError, "Unsupported entry kind %(kind)s"; kind);

/// UnexpectedInventoryFormat passes its message as the preformatted string
/// (via `BzrFormatsError.__init__(msg=...)`), so `str()` returns the message
/// verbatim rather than expanding `_fmt`.
#[pyclass(extends = BadInventoryFormat, dict, module = "bzrformats._bzr_rs.errors")]
pub struct UnexpectedInventoryFormat;

impl ErrInit for UnexpectedInventoryFormat {
    fn init_chain() -> PyClassInitializer<Self> {
        <BadInventoryFormat as ErrInit>::init_chain().add_subclass(UnexpectedInventoryFormat)
    }
}

#[pymethods]
impl UnexpectedInventoryFormat {
    #[classattr]
    fn _fmt() -> &'static str {
        "The inventory was not in the expected format:\n %(msg)s"
    }

    #[new]
    #[pyo3(signature = (*args, **kwds))]
    fn new(
        args: &Bound<'_, PyTuple>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        let _ = (args, kwds);
        <UnexpectedInventoryFormat as ErrInit>::init_chain()
    }

    fn __init__(slf: &Bound<'_, Self>, msg: Bound<'_, PyAny>) -> PyResult<()> {
        // Mirror serializer.py: super().__init__(msg=msg) sets the preformatted
        // string, so str() is the bare message.
        slf.setattr("_preformatted_string", msg)?;
        Ok(())
    }
}
simple_error!(KnitCorrupt: BzrFormatsError, "Knit %(knit)s corrupt: %(how)s"; knit, how);
simple_error!(KnitDataStreamIncompatible: BzrFormatsError, "Cannot insert knit data stream for %(key)s: %(msg)s"; key, msg);
simple_error!(KnitDataStreamUnknown: BzrFormatsError, "Unknown knit data stream for %(key)s"; key);
simple_error!(KnitHeaderError: BzrFormatsError, "Knit header error: %(badline)r"; badline);
simple_error!(BadIndexFormatSignature: BzrFormatsError, "%(value)s is not an index of type %(_type)s."; value, _type);
simple_error!(BadIndexData: BzrFormatsError, "Error in data for index %(value)s."; value);
simple_error!(BadIndexDuplicateKey: BzrFormatsError, "The key '%(key)s' is already in index '%(index)s'."; key, index);
simple_error!(BadIndexKey: BzrFormatsError, "The key '%(key)s' is not a valid key."; key);
simple_error!(BadIndexOptions: BzrFormatsError, "Could not parse options for index %(value)s."; value);
simple_error!(BadIndexValue: BzrFormatsError, "The value '%(value)s' is not a valid value."; value);
simple_error!(InvalidEntryName: BzrFormatsError, "Invalid entry name: %(name)s"; name);
simple_error!(DuplicateFileId: BzrFormatsError, "File id {%(file_id)s} already exists in inventory as %(entry)s"; file_id, entry);

simple_error!(VersionedFileError: BzrFormatsError, "Versioned file error");
simple_error!(RevisionNotPresent: VersionedFileError, "Revision {%(revision_id)s} not present in \"%(file_id)s\"."; revision_id, file_id);
simple_error!(RevisionAlreadyPresent: VersionedFileError, "Revision {%(revision_id)s} already present in \"%(file_id)s\"."; revision_id, file_id);
simple_error!(VersionedFileInvalidChecksum: VersionedFileError, "Text did not match its checksum: %(msg)s");

simple_error!(InvalidRevisionId: BzrFormatsError, "Invalid revision-id {%(revision_id)s} in %(branch)s"; revision_id, branch);
simple_error!(UnavailableRepresentation: BzrFormatsError, "The encoding '%(wanted)s' is not available for key %(key)s which is encoded as '%(native)s'."; key, wanted, native);
simple_error!(ExistingContent: BzrFormatsError, "The content being inserted is already present.");

// Weave errors. Definitions match the live weave.py (which diverged from the
// stale errors.py copies); breezy's weave_fmt plugin raises some of these.
simple_error!(WeaveRevisionAlreadyPresent: WeaveError, "Revision {%(revision_id)s} already present in %(weave)s"; revision_id, weave);
simple_error!(WeaveRevisionNotPresent: WeaveError, "Revision {%(revision_id)s} not present in %(weave)s"; revision_id, weave);
simple_error!(WeaveFormatError: WeaveError, "Weave invariant violated: %(what)s"; what);
simple_error!(WeaveParentMismatch: WeaveError, "Parents are mismatched between two revisions. %(msg)s");
simple_error!(WeaveInvalidChecksum: WeaveError, "Text did not match its checksum: %(msg)s");
simple_error!(WeaveTextDiffers: WeaveError, "Weaves differ on text content. Revision: {%(revision_id)s}, %(weave_a)s, %(weave_b)s"; revision_id, weave_a, weave_b);

/// WeaveError stores an optional `msg` (default None).
#[pyclass(extends = BzrFormatsError, subclass, dict, module = "bzrformats._bzr_rs.errors")]
pub struct WeaveError;

impl ErrInit for WeaveError {
    fn init_chain() -> PyClassInitializer<Self> {
        <BzrFormatsError as ErrInit>::init_chain().add_subclass(WeaveError)
    }
}

#[pymethods]
impl WeaveError {
    #[classattr]
    fn _fmt() -> &'static str {
        "Error in processing weave: %(msg)s"
    }

    #[new]
    #[pyo3(signature = (*args, **kwds))]
    fn new(
        args: &Bound<'_, PyTuple>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        let _ = (args, kwds);
        <WeaveError as ErrInit>::init_chain()
    }

    #[pyo3(signature = (msg = None))]
    fn __init__(slf: &Bound<'_, Self>, msg: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let py = slf.py();
        slf.setattr("msg", msg.unwrap_or_else(|| py.None().into_bound(py)))?;
        Ok(())
    }
}

simple_error!(ReservedId: BzrFormatsError, "Reserved revision-id {%(revision_id)s}"; revision_id);
simple_error!(BadFileKindError: BzrFormatsError, "Cannot operate on %(filename)s of unsupported kind %(kind)s"; filename, kind);

simple_error!(InternalBzrFormatsError: BzrFormatsError, "Internal error", internal_error = true);
simple_error!(BzrCheckError: InternalBzrFormatsError, "Internal check failed: %(msg)s"; msg);
simple_error!(NoSuchRevision: InternalBzrFormatsError, "%(branch)s has no revision %(revision)s"; branch, revision);

simple_error!(LockError: BzrFormatsError, "Lock error: %(msg)s", internal_error = false);
simple_error!(ObjectNotLocked: LockError, "%(obj)r is not locked"; obj);
simple_error!(ReadOnlyError: LockError, "A write attempt was made in a read only transaction on %(obj)s"; obj);
simple_error!(ReadOnlyObjectDirtiedError: ReadOnlyError, "Cannot change object %(obj)r in read only transaction"; obj);
simple_error!(OutSideTransaction: BzrFormatsError, "A transaction related operation was attempted after the transaction finished.");
simple_error!(LockNotHeld: LockError, "Lock not held: %(lock)s"; lock);

simple_error!(InconsistentDelta: BzrFormatsError, "An inconsistent delta was supplied involving %(path)r, %(file_id)r\nreason: %(reason)s"; path, file_id, reason);

/// PathError stores `extra` after prefixing it with ": " when truthy.
#[pyclass(extends = BzrFormatsError, subclass, dict, module = "bzrformats._bzr_rs.errors")]
pub struct PathError;

impl ErrInit for PathError {
    fn init_chain() -> PyClassInitializer<Self> {
        <BzrFormatsError as ErrInit>::init_chain().add_subclass(PathError)
    }
}

#[pymethods]
impl PathError {
    #[classattr]
    fn _fmt() -> &'static str {
        "Path error: %(path)r%(extra)s"
    }

    #[new]
    #[pyo3(signature = (*args, **kwds))]
    fn new(
        args: &Bound<'_, PyTuple>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        let _ = (args, kwds);
        <PathError as ErrInit>::init_chain()
    }

    #[pyo3(signature = (path, extra = None))]
    fn __init__(
        slf: &Bound<'_, Self>,
        path: Bound<'_, PyAny>,
        extra: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        slf.setattr("path", path)?;
        let extra_str = match extra {
            Some(e) if e.is_truthy()? => format!(": {}", e.str()?.to_str()?),
            _ => String::new(),
        };
        slf.setattr("extra", extra_str)?;
        Ok(())
    }
}

/// Define a PathError subclass that only overrides `_fmt`; it inherits
/// PathError's `(path, extra=None)` constructor and attribute handling.
macro_rules! path_error_subclass {
    ($(#[$meta:meta])* $name:ident, $fmt:expr) => {
        $(#[$meta])*
        #[pyclass(extends = PathError, subclass, dict, module = "bzrformats._bzr_rs.errors")]
        pub struct $name;

        impl ErrInit for $name {
            fn init_chain() -> PyClassInitializer<Self> {
                <PathError as ErrInit>::init_chain().add_subclass($name)
            }
        }

        #[pymethods]
        impl $name {
            #[classattr]
            fn _fmt() -> &'static str { $fmt }

            #[new]
            #[pyo3(signature = (*args, **kwds))]
            fn new(
                args: &Bound<'_, PyTuple>,
                kwds: Option<&Bound<'_, PyDict>>,
            ) -> PyClassInitializer<Self> {
                let _ = (args, kwds);
                <$name as ErrInit>::init_chain()
            }

            #[pyo3(signature = (path, extra = None))]
            fn __init__(
                slf: &Bound<'_, Self>,
                path: Bound<'_, PyAny>,
                extra: Option<Bound<'_, PyAny>>,
            ) -> PyResult<()> {
                slf.setattr("path", path)?;
                let extra_str = match extra {
                    Some(e) if e.is_truthy()? => format!(": {}", e.str()?.to_str()?),
                    _ => String::new(),
                };
                slf.setattr("extra", extra_str)?;
                Ok(())
            }
        }
    };
}

path_error_subclass!(
    /// Raised by transports when a file or directory does not exist.
    NoSuchFile, "No such file: %(path)r%(extra)s"
);
path_error_subclass!(
    InvalidNormalization,
    "Path \"%(path)s\" is not unicode normalized"
);

/// InconsistentDeltaDelta calls BzrFormatsError.__init__ directly (skipping the
/// parent's path/file_id handling) and stores delta + reason.
#[pyclass(extends = InconsistentDelta, dict, module = "bzrformats._bzr_rs.errors")]
pub struct InconsistentDeltaDelta;

impl ErrInit for InconsistentDeltaDelta {
    fn init_chain() -> PyClassInitializer<Self> {
        <InconsistentDelta as ErrInit>::init_chain().add_subclass(InconsistentDeltaDelta)
    }
}

#[pymethods]
impl InconsistentDeltaDelta {
    #[classattr]
    fn _fmt() -> &'static str {
        "An inconsistent delta was supplied: %(delta)r\nreason: %(reason)s"
    }

    #[new]
    #[pyo3(signature = (*args, **kwds))]
    fn new(
        args: &Bound<'_, PyTuple>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        let _ = (args, kwds);
        <InconsistentDeltaDelta as ErrInit>::init_chain()
    }

    fn __init__(
        slf: &Bound<'_, Self>,
        delta: Bound<'_, PyAny>,
        reason: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        slf.setattr("delta", delta)?;
        slf.setattr("reason", reason)?;
        Ok(())
    }
}

/// DecompressCorruption prefixes a non-empty orig_error with ", ".
#[pyclass(extends = BzrFormatsError, dict, module = "bzrformats._bzr_rs.errors")]
pub struct DecompressCorruption;

impl ErrInit for DecompressCorruption {
    fn init_chain() -> PyClassInitializer<Self> {
        <BzrFormatsError as ErrInit>::init_chain().add_subclass(DecompressCorruption)
    }
}

#[pymethods]
impl DecompressCorruption {
    #[classattr]
    fn _fmt() -> &'static str {
        "Corruption while decompressing repository file%(orig_error)s"
    }

    #[new]
    #[pyo3(signature = (*args, **kwds))]
    fn new(
        args: &Bound<'_, PyTuple>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        let _ = (args, kwds);
        <DecompressCorruption as ErrInit>::init_chain()
    }

    #[pyo3(signature = (orig_error = None))]
    fn __init__(slf: &Bound<'_, Self>, orig_error: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let value = match orig_error {
            Some(e) if e.is_truthy()? => format!(", {}", e.str()?.to_str()?),
            _ => String::new(),
        };
        slf.setattr("orig_error", value)?;
        Ok(())
    }
}

/// LockContention has an optional `msg` defaulting to "".
#[pyclass(extends = LockError, dict, module = "bzrformats._bzr_rs.errors")]
pub struct LockContention;

impl ErrInit for LockContention {
    fn init_chain() -> PyClassInitializer<Self> {
        <LockError as ErrInit>::init_chain().add_subclass(LockContention)
    }
}

#[pymethods]
impl LockContention {
    #[classattr]
    fn _fmt() -> &'static str {
        "Could not acquire lock \"%(lock)s\": %(msg)s"
    }

    #[new]
    #[pyo3(signature = (*args, **kwds))]
    fn new(
        args: &Bound<'_, PyTuple>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        let _ = (args, kwds);
        <LockContention as ErrInit>::init_chain()
    }

    #[pyo3(signature = (lock, msg = None))]
    fn __init__(
        slf: &Bound<'_, Self>,
        lock: Bound<'_, PyAny>,
        msg: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        slf.setattr("lock", lock)?;
        let py = slf.py();
        let msg = msg.unwrap_or_else(|| PyString::new(py, "").into_any());
        slf.setattr("msg", msg)?;
        Ok(())
    }
}

/// AlreadyVersionedError formats an optional context_info prefix.
#[pyclass(extends = BzrFormatsError, dict, module = "bzrformats._bzr_rs.errors")]
pub struct AlreadyVersionedError;

impl ErrInit for AlreadyVersionedError {
    fn init_chain() -> PyClassInitializer<Self> {
        <BzrFormatsError as ErrInit>::init_chain().add_subclass(AlreadyVersionedError)
    }
}

#[pymethods]
impl AlreadyVersionedError {
    #[classattr]
    fn _fmt() -> &'static str {
        "%(context_info)s%(path)s is already versioned."
    }

    #[new]
    #[pyo3(signature = (*args, **kwds))]
    fn new(
        args: &Bound<'_, PyTuple>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        let _ = (args, kwds);
        <AlreadyVersionedError as ErrInit>::init_chain()
    }

    #[pyo3(signature = (path, context_info = None))]
    fn __init__(
        slf: &Bound<'_, Self>,
        path: Bound<'_, PyAny>,
        context_info: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        slf.setattr("path", path)?;
        let value = match context_info {
            Some(c) if !c.is_none() => format!("{}. ", c.str()?.to_str()?),
            _ => String::new(),
        };
        slf.setattr("context_info", value)?;
        Ok(())
    }
}

/// NotVersionedError formats an optional context_info prefix (default "").
#[pyclass(extends = BzrFormatsError, dict, module = "bzrformats._bzr_rs.errors")]
pub struct NotVersionedError;

impl ErrInit for NotVersionedError {
    fn init_chain() -> PyClassInitializer<Self> {
        <BzrFormatsError as ErrInit>::init_chain().add_subclass(NotVersionedError)
    }
}

#[pymethods]
impl NotVersionedError {
    #[classattr]
    fn _fmt() -> &'static str {
        "%(context_info)s%(path)s is not versioned."
    }

    #[new]
    #[pyo3(signature = (*args, **kwds))]
    fn new(
        args: &Bound<'_, PyTuple>,
        kwds: Option<&Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        let _ = (args, kwds);
        <NotVersionedError as ErrInit>::init_chain()
    }

    #[pyo3(signature = (path, context_info = None))]
    fn __init__(
        slf: &Bound<'_, Self>,
        path: Bound<'_, PyAny>,
        context_info: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        slf.setattr("path", path)?;
        let value = match context_info {
            Some(c) if c.is_truthy()? => format!("{}. ", c.str()?.to_str()?),
            _ => String::new(),
        };
        slf.setattr("context_info", value)?;
        Ok(())
    }
}

/// Build the `errors` submodule.
pub(crate) fn errors_module(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "errors")?;
    m.add_class::<BzrFormatsError>()?;
    m.add_class::<UnexpectedInventoryFormat>()?;
    m.add_class::<UnsupportedInventoryKind>()?;
    m.add_class::<KnitCorrupt>()?;
    m.add_class::<KnitDataStreamIncompatible>()?;
    m.add_class::<KnitDataStreamUnknown>()?;
    m.add_class::<KnitHeaderError>()?;
    m.add_class::<BadIndexFormatSignature>()?;
    m.add_class::<BadIndexData>()?;
    m.add_class::<BadIndexDuplicateKey>()?;
    m.add_class::<BadIndexKey>()?;
    m.add_class::<BadIndexOptions>()?;
    m.add_class::<BadIndexValue>()?;
    m.add_class::<InvalidEntryName>()?;
    m.add_class::<DuplicateFileId>()?;
    m.add_class::<DecompressCorruption>()?;
    m.add_class::<VersionedFileError>()?;
    m.add_class::<RevisionNotPresent>()?;
    m.add_class::<RevisionAlreadyPresent>()?;
    m.add_class::<VersionedFileInvalidChecksum>()?;
    m.add_class::<InvalidRevisionId>()?;
    m.add_class::<UnavailableRepresentation>()?;
    m.add_class::<ExistingContent>()?;
    m.add_class::<WeaveError>()?;
    m.add_class::<WeaveRevisionAlreadyPresent>()?;
    m.add_class::<WeaveRevisionNotPresent>()?;
    m.add_class::<WeaveFormatError>()?;
    m.add_class::<WeaveParentMismatch>()?;
    m.add_class::<WeaveInvalidChecksum>()?;
    m.add_class::<WeaveTextDiffers>()?;
    m.add_class::<BadInventoryFormat>()?;
    m.add_class::<ReservedId>()?;
    m.add_class::<BadFileKindError>()?;
    m.add_class::<PathError>()?;
    m.add_class::<NoSuchFile>()?;
    m.add_class::<InvalidNormalization>()?;
    m.add_class::<InconsistentDelta>()?;
    m.add_class::<InconsistentDeltaDelta>()?;
    m.add_class::<InternalBzrFormatsError>()?;
    m.add_class::<BzrCheckError>()?;
    m.add_class::<NoSuchRevision>()?;
    m.add_class::<LockError>()?;
    m.add_class::<ObjectNotLocked>()?;
    m.add_class::<ReadOnlyError>()?;
    m.add_class::<ReadOnlyObjectDirtiedError>()?;
    m.add_class::<OutSideTransaction>()?;
    m.add_class::<LockContention>()?;
    m.add_class::<LockNotHeld>()?;
    m.add_class::<AlreadyVersionedError>()?;
    m.add_class::<NotVersionedError>()?;
    Ok(m)
}
