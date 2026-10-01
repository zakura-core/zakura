fn main() {
    println!("cargo:rerun-if-changed=cxx");
    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .include("cxx/zcash")
        .file("cxx/count.cpp")
        .compile("script_oracle_count");
}
