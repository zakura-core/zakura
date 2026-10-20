//! Platform directories used for cache, configuration, and node identity.

use std::path::PathBuf;

/// Returns the user's platform home directory.
///
/// On supported Unix platforms, a nonempty `HOME` takes precedence over the
/// account database, and empty account paths are ignored. On
/// Windows, this uses the profile known folder rather than environment variables.
pub fn home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        known_folder(windows_sys::Win32::UI::Shell::FOLDERID_Profile)
    }
    #[cfg(target_arch = "wasm32")]
    {
        None
    }
    #[cfg(not(any(windows, target_arch = "wasm32")))]
    {
        std::env::home_dir().filter(|path| !path.as_os_str().is_empty())
    }
}

/// Returns the platform preferences directory used for config discovery.
pub fn preference_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        known_folder(windows_sys::Win32::UI::Shell::FOLDERID_LocalAppData)
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        home_dir().map(|home| home.join("Library/Preferences"))
    }
    #[cfg(target_arch = "wasm32")]
    {
        None
    }
    #[cfg(not(any(
        windows,
        target_os = "macos",
        target_os = "ios",
        target_arch = "wasm32"
    )))]
    {
        xdg_dir("XDG_CONFIG_HOME", ".config")
    }
}

/// Returns the platform cache directory without Zakura's suffix.
pub(super) fn cache_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        known_folder(windows_sys::Win32::UI::Shell::FOLDERID_LocalAppData)
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        home_dir().map(|home| home.join("Library/Caches"))
    }
    #[cfg(target_arch = "wasm32")]
    {
        None
    }
    #[cfg(not(any(
        windows,
        target_os = "macos",
        target_os = "ios",
        target_arch = "wasm32"
    )))]
    {
        xdg_dir("XDG_CACHE_HOME", ".cache")
    }
}

#[cfg(not(any(
    windows,
    target_os = "macos",
    target_os = "ios",
    target_arch = "wasm32"
)))]
fn xdg_dir(variable: &str, suffix: &str) -> Option<PathBuf> {
    std::env::var_os(variable)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| home_dir().map(|home| home.join(suffix)))
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn known_folder(folder: windows_sys::core::GUID) -> Option<PathBuf> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt, ptr, slice};
    use windows_sys::Win32::{
        Globalization::lstrlenW, System::Com::CoTaskMemFree, UI::Shell::SHGetKnownFolderPath,
    };

    struct FolderPath(windows_sys::core::PWSTR);

    impl Drop for FolderPath {
        fn drop(&mut self) {
            // SAFETY: the shell allocates this buffer with the COM allocator;
            // freeing a null pointer is also permitted.
            unsafe { CoTaskMemFree(self.0.cast()) };
        }
    }

    let mut path = FolderPath(ptr::null_mut());
    // SAFETY: the GUID and output pointer are valid for this call. A null token
    // requests the current user's folder. The guard owns any returned allocation.
    let status = unsafe { SHGetKnownFolderPath(&folder, 0, ptr::null_mut(), &mut path.0) };
    if status != 0 || path.0.is_null() {
        return None;
    }

    // SAFETY: a successful shell lookup returns a null-terminated UTF-16 string.
    let length = usize::try_from(unsafe { lstrlenW(path.0) }).ok()?;
    // SAFETY: the allocation contains these UTF-16 units and remains alive until
    // the path is copied below and the guard is dropped.
    let units = unsafe { slice::from_raw_parts(path.0, length) };
    Some(PathBuf::from(OsString::from_wide(units)))
}

#[cfg(test)]
mod tests;
