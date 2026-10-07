use super::*;

#[test]
fn encode_decode_round_trips() {
    for arch in ARCHITECTURES {
        let t = table(arch).unwrap();
        let all: Vec<String> = t.names().map(String::from).collect();
        let mask = encode_cpu_features(arch, &all).unwrap();
        assert_eq!(decode_cpu_features(arch, &mask).unwrap(), all, "{arch}");

        let empty: [&str; 0] = [];
        let mask = encode_cpu_features(arch, &empty).unwrap();
        assert_eq!(mask, "0", "{arch}");
        assert!(decode_cpu_features(arch, &mask).unwrap().is_empty());
    }
}

#[test]
fn duplicates_and_order_are_normalized() {
    let m1 = encode_cpu_features("amd64", &["avx2", "pni", "avx2", ""]).unwrap();
    let m2 = encode_cpu_features("amd64", &["pni", "avx2"]).unwrap();
    assert_eq!(m1, m2);
    assert_eq!(decode_cpu_features("amd64", &m1).unwrap(), ["pni", "avx2"]);
}

#[test]
fn encode_and_decode_errors() {
    assert!(encode_cpu_features("riscv64", &["pni"]).is_err());
    assert!(encode_cpu_features("amd64", &["not_a_real_feature"]).is_err());
    assert!(decode_cpu_features("riscv64", "0").is_err());
    let too_long = "f".repeat(MASK_WORDS * 16 + 1);
    for bad in ["", "0x1f", "1F", "07", "xyz", too_long.as_str()] {
        assert!(
            decode_cpu_features("amd64", bad).is_err(),
            "{bad:?} must be rejected as non-canonical"
        );
    }
    let amd64 = table("amd64").unwrap();
    let last = amd64.bits().last().unwrap();
    for bit in [16, last + 1] {
        assert!(
            amd64.name(bit).is_none(),
            "test assumes bit {bit} is unnamed"
        );
        let mut m = FeatureMask::default();
        m.set(0);
        m.set(bit);
        let err = decode_cpu_features("amd64", &m.to_string()).unwrap_err();
        assert!(err.to_string().contains(&bit.to_string()), "{err}");
    }
}

#[test]
fn canonical_hex() {
    let mut m = FeatureMask::default();
    for bit in [0, 1, 2, 64] {
        m.set(bit);
    }
    let want = format!("1{}7", "0".repeat(15));
    assert_eq!(m.to_string(), want);
    assert_eq!(FeatureMask::parse(&want), Some(m));
}

#[test]
fn tables_hold_the_documented_bits() {
    for (arch, name, bit) in [
        ("amd64", "pni", 0),
        ("amd64", "pcid", 17),
        ("amd64", "osxsave", 27),
        ("amd64", "fpu", 32),
        ("amd64", "avx2", 2 * 32 + 5),
        ("amd64", "syscall", 6 * 32 + 11),
        ("amd64", "avx512_bf16", 8 * 32 + 5),
        ("arm64", "fp", 0),
        ("arm64", "asimdfhm", 23),
    ] {
        assert_eq!(table(arch).unwrap().bit(name), Some(bit), "{arch} {name}");
    }
    for arch in ARCHITECTURES {
        let t = table(arch).unwrap();
        assert!(!t.is_empty());
        let bits: Vec<usize> = t.bits().collect();
        assert!(bits.windows(2).all(|w| w[0] < w[1]));
        let mut defined = FeatureMask::default();
        let mut seen = HashMap::new();
        for bit in bits {
            let name = t.name(bit).unwrap();
            defined.set(bit);
            assert!(
                seen.insert(name, bit).is_none(),
                "{arch} lists {name} twice"
            );
            assert!(!name.is_empty() && name == name.trim());
            assert_eq!(t.bit(name), Some(bit));
        }
        assert_eq!(defined, t.defined());
        assert_eq!(seen.len(), t.len());
    }
}

#[test]
#[should_panic(expected = "outside")]
fn a_table_bit_outside_the_mask_panics() {
    FeatureTable::new(&[(MASK_WORDS * 64, "beyond")]);
}

#[test]
fn parses_cpuinfo() {
    let intel = "processor\t: 0\nvendor_id\t: GenuineIntel\ncpu family\t: 6\nmodel\t\t: 143\nmodel name\t: Intel(R) Xeon(R) Platinum 8481C\n";
    assert_eq!(
        parse_cpu_info("amd64", intel.as_bytes()).unwrap(),
        ("GenuineIntel".into(), "6:143".into())
    );
    let neoverse = "processor\t: 0\nBogoMIPS\t: 2000.00\nFeatures\t: fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics\nCPU implementer\t: 0x41\nCPU architecture: 8\nCPU variant\t: 0x0\nCPU part\t: 0xd40\nCPU revision\t: 1\n";
    assert_eq!(
        parse_cpu_info("arm64", neoverse.as_bytes()).unwrap(),
        ("0x41".into(), "0x0:0xd40:1".into())
    );
    assert!(parse_cpu_info("amd64", "vendor_id : GenuineIntel\n".as_bytes()).is_err());
    assert!(parse_cpu_info("riscv64", "vendor_id : test\n".as_bytes()).is_err());
}

/// Real CPUs' feature lists: decoding inverts encoding, and the mask subset
/// agrees with set inclusion of the decoded names.
#[test]
fn real_cpu_feature_sets_compare_by_subset() {
    const BROADWELL: &str = "fpu,vme,de,pse,tsc,msr,pae,mce,cx8,apic,sep,mtrr,pge,mca,cmov,pat,pse36,clflush,mmx,fxsr,sse,sse2,ss,ht,syscall,nx,pdpe1gb,rdtscp,lm,pni,pclmulqdq,ssse3,fma,cx16,pcid,sse4_1,sse4_2,x2apic,movbe,popcnt,aes,xsave,osxsave,avx,f16c,rdrand,hypervisor,lahf_lm,abm,3dnowprefetch,fsgsbase,tsc_adjust,bmi1,hle,avx2,smep,bmi2,erms,invpcid,rtm,rdseed,adx,smap,xsaveopt,md_clear,arch_capabilities";
    const SAPPHIRE_RAPIDS: &str = "fpu,vme,de,pse,tsc,msr,pae,mce,cx8,apic,sep,mtrr,pge,mca,cmov,pat,pse36,clflush,mmx,fxsr,sse,sse2,ss,ht,syscall,nx,pdpe1gb,rdtscp,lm,pni,pclmulqdq,ssse3,fma,cx16,pcid,sse4_1,sse4_2,x2apic,movbe,popcnt,tsc_deadline_timer,aes,xsave,osxsave,avx,f16c,rdrand,hypervisor,lahf_lm,abm,3dnowprefetch,fsgsbase,tsc_adjust,bmi1,hle,avx2,smep,bmi2,erms,invpcid,rtm,avx512f,avx512dq,rdseed,adx,smap,clfushopt,clwb,avx512cd,sha_ni,avx512bw,avx512vl,xsaveopt,xsavec,xgetbv1,xsaves,avx512vbmi,umip,pku,ospke,waitpkg,avx512_vbmi2,gfni,vaes,vpclmulqdq,avx512_vnni,avx512_bitalg,avx512_vpopcntdq,la57,rdpid,cldemote,movdiri,movdir64b,fsrm,md_clear,serialize,tsxldtrk,amx_bf16,avx512_fp16,amx_tile,amx_int8,spec_ctrl,intel_stibp,flush_l1d,arch_capabilities,spec_ctrl_ssbd,avx_vnni,avx512_bf16";
    const AMD_MILAN: &str = "fpu,vme,de,pse,tsc,msr,pae,mce,cx8,apic,sep,mtrr,pge,mca,cmov,pat,pse36,clflush,mmx,fxsr,sse,sse2,ht,syscall,nx,mmxext,fxsr_opt,pdpe1gb,rdtscp,lm,pni,pclmulqdq,ssse3,fma,cx16,pcid,sse4_1,sse4_2,x2apic,movbe,popcnt,aes,xsave,osxsave,avx,f16c,rdrand,hypervisor,lahf_lm,cmp_legacy,svm,cr8_legacy,abm,sse4a,misalignsse,3dnowprefetch,osvw,topoext,perfctr_core,fsgsbase,tsc_adjust,bmi1,avx2,smep,bmi2,erms,invpcid,rdseed,adx,smap,clfushopt,clwb,sha_ni,xsaveopt,xsavec,xgetbv1,xsaves,umip,pku,ospke,vaes,vpclmulqdq,rdpid,fsrm";
    const NEOVERSE_N1: &str =
        "fp,asimd,evtstrm,aes,pmull,sha1,sha2,crc32,atomics,fphp,asimdhp,cpuid,asimdrdm,lrcpc,dcpop,asimddp";
    const NEOVERSE_V2: &str = "fp,asimd,evtstrm,aes,pmull,sha1,sha2,crc32,atomics,fphp,asimdhp,cpuid,asimdrdm,jscvt,fcma,lrcpc,dcpop,sha3,sm3,sm4,asimddp,sha512,sve,asimdfhm";
    for (arch, csv) in [
        ("amd64", BROADWELL),
        ("amd64", SAPPHIRE_RAPIDS),
        ("amd64", AMD_MILAN),
        ("arm64", NEOVERSE_N1),
        ("arm64", NEOVERSE_V2),
    ] {
        let names: Vec<&str> = csv.split(',').collect();
        encode_cpu_features(arch, &names).unwrap_or_else(|e| panic!("{arch}: {e}"));
    }
    for (arch, snap, worker) in [
        ("amd64", BROADWELL, SAPPHIRE_RAPIDS),
        ("amd64", SAPPHIRE_RAPIDS, BROADWELL),
        ("amd64", BROADWELL, AMD_MILAN),
        ("arm64", NEOVERSE_N1, NEOVERSE_V2),
        ("arm64", NEOVERSE_V2, NEOVERSE_N1),
        ("amd64", "fpu,sse2,constant_tsc,rep_good,nopl", BROADWELL),
    ] {
        let snap: Vec<&str> = snap.split(',').collect();
        let worker: Vec<&str> = worker.split(',').collect();
        let (Ok(sm), Ok(wm)) = (
            encode_cpu_features(arch, &snap),
            encode_cpu_features(arch, &worker),
        ) else {
            continue;
        };
        let sd = decode_cpu_features(arch, &sm).unwrap();
        assert_eq!(encode_cpu_features(arch, &sd).unwrap(), sm);
        let wd = decode_cpu_features(arch, &wm).unwrap();
        let want = sd.iter().all(|f| wd.contains(f));
        let (s, w) = (
            FeatureMask::parse(&sm).unwrap(),
            FeatureMask::parse(&wm).unwrap(),
        );
        assert_eq!(s.to_string(), sm);
        assert_eq!(s.subset_of(&w), want, "{sm} vs {wm}");
    }
}
