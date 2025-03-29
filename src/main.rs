use anyhow::{anyhow, Result};
use bytesize::ByteSize;
use chrono::Local;
use clap::Parser;
use humantime::format_duration;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use sysinfo::{System, SystemExt, ProcessExt};
use walkdir::WalkDir;
use twox_hash::xxh3::hash64;

#[derive(Parser, Debug)]
#[command(author, version, about = "Media File Deduplication Tool")]
struct Args {
    #[arg(short, long)]
    filepath: Option<PathBuf>,
}

const VIDEO_FORMATS: [&str; 11] = [
    "mp4", "flv", "mkv", "avi", "mov", "wmv", "webm", "m4v", "mpg", "mpeg", "ts",
];

struct MediaDeduplicator {
    root_path: PathBuf,
    script_dir: PathBuf,
    checksum_db_path: PathBuf,
    destructive_script_path: PathBuf,
    
    checksum_to_file: HashMap<String, String>,
    checksum_to_files: HashMap<String, Vec<String>>,
    basename_map: HashSet<String>,
    dir_dupes: HashMap<String, Vec<String>>,
    cross_dir_dupes: HashSet<String>,
    
    total_files: usize,
    unique_files: usize,
    same_dir_dupes: usize,
    cross_dir_dupes_count: usize,
    rename_candidates: usize,
    
    // Performance metrics
    start_time: Instant,
    hashing_time: Duration,
    total_bytes_processed: u64,
    peak_memory_usage: u64,
    system_info: System,
}

impl MediaDeduplicator {
    fn new() -> Result<Self> {
        let current_dir = env::current_dir()?;
        Ok(Self {
            root_path: current_dir.clone(),
            script_dir: current_dir.clone(),
            checksum_db_path: current_dir.join("sha256sum.txt"),
            destructive_script_path: current_dir.join("potentially-destructive-remove.sh"),
            
            checksum_to_file: HashMap::new(),
            checksum_to_files: HashMap::new(),
            basename_map: HashSet::new(),
            dir_dupes: HashMap::new(),
            cross_dir_dupes: HashSet::new(),
            
            total_files: 0,
            unique_files: 0,
            same_dir_dupes: 0,
            cross_dir_dupes_count: 0,
            rename_candidates: 0,
            
            // Initialize performance metrics
            start_time: Instant::now(),
            hashing_time: Duration::from_secs(0),
            total_bytes_processed: 0,
            peak_memory_usage: 0,
            system_info: System::new_all(),
        })
    }
    
    fn run(&mut self, args: Args) -> Result<()> {
        if let Some(dir) = args.filepath {
            self.root_path = fs::canonicalize(dir)?;
        }
        
        println!("Working directory: {}", self.root_path.display());
        
        if !self.checksum_db_path.exists() {
            println!("Checksum database not found at {}", self.checksum_db_path.display());
            println!("Creating new database file...");
            File::create(&self.checksum_db_path)?;
        }
        
        let dirs_to_process = self.find_media_dirs()?;
        println!("Found {} directories with media files", dirs_to_process.len());
        
        self.init_destructive_script()?;
        
        self.load_database()?;
        
        println!("First pass: collecting file information...");
        self.process_all_directories(&dirs_to_process)?;
        
        println!("\nSecond pass: analyzing duplicates and preparing actions...");
        self.analyze_within_directory_duplicates()?;
        self.analyze_cross_directory_duplicates()?;
        self.analyze_rename_candidates(&dirs_to_process)?;
        
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&self.destructive_script_path)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&self.destructive_script_path, perms)?;
        }
        
        self.display_results();
        
        Ok(())
    }
    
    fn find_media_dirs(&self) -> Result<Vec<PathBuf>> {
        println!("Identifying directories containing media files...");
        let mut dirs = vec![self.root_path.clone()];
        
        for entry in WalkDir::new(&self.root_path)
            .min_depth(1)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_dir())
        {
            let dir_path = entry.path();
            
            let has_media = VIDEO_FORMATS.iter().any(|&format| {
                dir_path.read_dir().map_or(false, |entries| {
                    entries
                        .filter_map(Result::ok)
                        .any(|e| {
                            e.file_type().map_or(false, |ft| ft.is_file())
                                && e.path().extension().map_or(false, |ext| 
                                    ext.to_string_lossy().to_lowercase() == format)
                        })
                })
            });
            
            if has_media {
                dirs.push(dir_path.to_path_buf());
            }
        }
        
        Ok(dirs)
    }
    
    fn init_destructive_script(&self) -> Result<()> {
        let mut file = File::create(&self.destructive_script_path)?;
        
        writeln!(file, "#!/usr/bin/env bash")?;
        writeln!(file, "")?;
        writeln!(file, "# WARNING: This script contains potentially destructive operations")?;
        writeln!(file, "# Review carefully before running!")?;
        writeln!(file, "# Generated on {}", Local::now().format("%Y-%m-%d %H:%M:%S"))?;
        writeln!(file, "")?;
        writeln!(file, "# Set to exit on error")?;
        writeln!(file, "set -e")?;
        writeln!(file, "")?;
        writeln!(file, "# Function to create directory structure")?;
        writeln!(file, "create_parent_dirs() {{")?;
        writeln!(file, "    local file=\"$1\"")?;
        writeln!(file, "    local target_dir=\"$2\"")?;
        writeln!(file, "    local parent_dir=\"$(dirname \"$file\")\"")?;
        writeln!(file, "    if [ \"$parent_dir\" != \".\" ]; then")?;
        writeln!(file, "        mkdir -p \"$target_dir/$parent_dir\"")?;
        writeln!(file, "    fi")?;
        writeln!(file, "}}")?;
        writeln!(file, "")?;
        writeln!(file, "# Create backup directory")?;
        writeln!(
            file,
            "BACKUP_DIR=\"{}/backup_{}\"",
            self.script_dir.display(),
            Local::now().format("%Y%m%d_%H%M%S")
        )?;
        writeln!(file, "mkdir -p \"$BACKUP_DIR\"")?;
        writeln!(file, "")?;
        writeln!(file, "# Operations are grouped by directory for easier review")?;
        writeln!(file, "")?;
        
        Ok(())
    }
    
    fn load_database(&mut self) -> Result<()> {
        if !self.checksum_db_path.exists() {
            return Ok(());
        }
        
        // Always start fresh - don't reuse old hash database to avoid stale data
        // Just create a backup of the old database and start new
        let backup_path = self.checksum_db_path.with_extension("txt.bak");
        if self.checksum_db_path.exists() {
            println!("Backing up old checksum database to {}", backup_path.display());
            fs::copy(&self.checksum_db_path, &backup_path)?;
            // Truncate the existing file to start fresh
            File::create(&self.checksum_db_path)?;
        }
        
        // We'll recalculate all hashes for the current files
        println!("Starting with a fresh checksum database");
        
        Ok(())
    }
    
    fn process_all_directories(&mut self, dirs: &[PathBuf]) -> Result<()> {
        for dir_path in dirs {
            let dir_name = self.get_relative_path(dir_path);
            let display_name = if dir_name.is_empty() { "root".to_string() } else { dir_name.clone() };
            
            println!("Examining directory: {}", display_name);
            
            let mut media_files = Vec::new();
            
            for entry in fs::read_dir(dir_path)? {
                let entry = entry?;
                let path = entry.path();
                
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_string_lossy().to_lowercase();
                        if VIDEO_FORMATS.contains(&ext_str.as_ref()) {
                            media_files.push(path);
                        }
                    }
                }
            }
            
            println!("Found {} media files in {}", media_files.len(), display_name);
            
            for media_path in media_files {
                self.total_files += 1;
                
                let media_filename = media_path.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                
                // Always calculate a fresh checksum
                let file_checksum = self.calculate_hash(&media_path)?;
                println!("Calculating checksum: {} ({}...)", media_filename, &file_checksum[..8]);
                
                // Update the database with the fresh checksum
                self.add_to_database(&media_path, &file_checksum)?;
                
                if !self.checksum_to_file.contains_key(&file_checksum) {
                    self.checksum_to_file.insert(
                        file_checksum.clone(),
                        media_path.to_string_lossy().into_owned(),
                    );
                    self.checksum_to_files.entry(file_checksum).or_insert_with(Vec::new)
                        .push(media_path.to_string_lossy().into_owned());
                    self.unique_files += 1;
                } else {
                    let media_path_str = media_path.to_string_lossy().into_owned();
                    self.checksum_to_files.entry(file_checksum.clone()).or_insert_with(Vec::new)
                        .push(media_path_str.clone());
                    
                    let existing_file = self.checksum_to_file.get(&file_checksum).unwrap();
                    let existing_dir = self.get_dir_path(existing_file);
                    let current_dir = self.get_dir_path(&media_path_str);
                    
                    if existing_dir == current_dir {
                        self.same_dir_dupes += 1;
                        self.dir_dupes.entry(current_dir).or_insert_with(Vec::new)
                            .push(file_checksum.clone());
                    } else {
                        self.cross_dir_dupes_count += 1;
                        self.cross_dir_dupes.insert(file_checksum);
                    }
                }
                
                if self.has_numeric_suffix(&media_filename) {
                    self.rename_candidates += 1;
                }
            }
        }
        
        Ok(())
    }
    
    fn analyze_within_directory_duplicates(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.destructive_script_path)?;
        
        writeln!(file, "###")?;
        writeln!(file, "# Within-Directory Duplicates")?;
        writeln!(file, "###")?;
        writeln!(file, "")?;
        
        for (dir, checksums) in &self.dir_dupes {
            writeln!(file, "# Processing directory: {}", dir)?;
            writeln!(file, "mkdir -p \"$BACKUP_DIR/{}/\"", dir)?;
            writeln!(file, "")?;
            
            for checksum in checksums {
                let all_files = self.checksum_to_files.get(checksum).unwrap();
                
                let dir_files: Vec<&String> = all_files.iter()
                    .filter(|&file| self.get_dir_path(file) == *dir)
                    .collect();
                
                if dir_files.len() > 1 {
                    let mut keep_file = "";
                    let mut longest_len = 0;
                    
                    for &file in &dir_files {
                        let filename = Path::new(file).file_name()
                            .unwrap_or_default()
                            .to_string_lossy();
                        
                        if !self.has_numeric_suffix(&filename) {
                            keep_file = file;
                            break;
                        }
                        
                        let file_len = filename.len();
                        if file_len > longest_len {
                            longest_len = file_len;
                            keep_file = file;
                        }
                    }
                    
                    if keep_file.is_empty() && !dir_files.is_empty() {
                        keep_file = dir_files[0];
                    }
                    
                    writeln!(file, "# Duplicate set with checksum: {}...", &checksum[..8])?;
                    writeln!(file, "# Keeping: {}", Path::new(keep_file).file_name().unwrap_or_default().to_string_lossy())?;
                    
                    for &file_path in &dir_files {
                        if file_path != keep_file {
                            let filename = Path::new(file_path).file_name()
                                .unwrap_or_default()
                                .to_string_lossy();
                            
                            writeln!(file, "# Backup and remove: {}", filename)?;
                            writeln!(file, "cp \"{}\" \"$BACKUP_DIR/{}/{}\"", file_path, dir, filename)?;
                            writeln!(file, "rm \"{}\"", file_path)?;
                        }
                    }
                    
                    writeln!(file, "")?;
                }
            }
        }
        
        Ok(())
    }
    
    fn analyze_cross_directory_duplicates(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.destructive_script_path)?;
        
        writeln!(file, "")?;
        writeln!(file, "###")?;
        writeln!(file, "# Cross-Directory Duplicates")?;
        writeln!(file, "###")?;
        writeln!(file, "")?;
        writeln!(file, "# WARNING: These are duplicates across different directories.")?;
        writeln!(file, "# The script does not automatically remove them as they may serve different purposes.")?;
        writeln!(file, "# Review and uncomment the sections below if you want to remove them.")?;
        writeln!(file, "")?;
        
        for checksum in &self.cross_dir_dupes {
            let all_files = self.checksum_to_files.get(checksum).unwrap();
            
            writeln!(file, "# Duplicate set with checksum: {}...", &checksum[..8])?;
            writeln!(file, "# First encountered: {} in {}", 
                Path::new(&all_files[0]).file_name().unwrap_or_default().to_string_lossy(),
                self.get_dir_path(&all_files[0]))?;
            writeln!(file, "# Other copies:")?;
            
            for i in 1..all_files.len() {
                let file_path = &all_files[i];
                let file_dir = self.get_dir_path(file_path);
                let filename = Path::new(file_path).file_name()
                    .unwrap_or_default()
                    .to_string_lossy();
                
                writeln!(file, "# {} in {}", filename, file_dir)?;
                writeln!(file, "# cp \"{}\" \"$BACKUP_DIR/{}/{}\"", file_path, file_dir, filename)?;
                writeln!(file, "# rm \"{}\"", file_path)?;
                writeln!(file, "#")?;
            }
            
            writeln!(file, "")?;
        }
        
        Ok(())
    }
    
    fn analyze_rename_candidates(&mut self, dirs: &[PathBuf]) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.destructive_script_path)?;
        
        writeln!(file, "")?;
        writeln!(file, "###")?;
        writeln!(file, "# Filename Cleanup (Remove Numeric Suffixes)")?;
        writeln!(file, "###")?;
        writeln!(file, "")?;
        writeln!(file, "# Files with numeric suffixes can be renamed to cleaner versions")?;
        writeln!(file, "# Be careful with these operations to avoid name conflicts")?;
        writeln!(file, "")?;
        
        // Create a set of files that are duplicates within the same directory
        let mut duplicate_files = HashSet::new();
        for (dir, checksums) in &self.dir_dupes {
            for checksum in checksums {
                let all_files = self.checksum_to_files.get(checksum).unwrap();
                
                let dir_files: Vec<&String> = all_files.iter()
                    .filter(|&file| self.get_dir_path(file) == *dir)
                    .collect();
                
                if dir_files.len() > 1 {
                    // These are duplicates within the same directory
                    for &file_path in &dir_files {
                        duplicate_files.insert(file_path.to_string());
                    }
                }
            }
        }
        
        for dir_path in dirs {
            let dir_name = self.get_relative_path(dir_path);
            let display_name = if dir_name.is_empty() { "root".to_string() } else { dir_name.clone() };
            
            let mut rename_files = Vec::new();
            
            for entry in fs::read_dir(dir_path)? {
                let entry = entry?;
                let path = entry.path();
                
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_string_lossy().to_lowercase();
                        if VIDEO_FORMATS.contains(&ext_str.as_ref()) {
                            let filename = path.file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .into_owned();
                            
                            // Only consider renaming files that are duplicates
                            let path_str = path.to_string_lossy().into_owned();
                            if self.has_numeric_suffix(&filename) && duplicate_files.contains(&path_str) {
                                rename_files.push(path);
                            }
                        }
                    }
                }
            }
            
            if !rename_files.is_empty() {
                writeln!(file, "# Directory: {}", display_name)?;
                writeln!(file, "mkdir -p \"$BACKUP_DIR/{}\"", display_name)?;
                writeln!(file, "")?;
                
                for file_path in rename_files {
                    let filename = file_path.file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned();
                    let clean_name = self.remove_numeric_suffix(&filename);
                    
                    let mut conflict = false;
                    
                    if self.basename_map.contains(&clean_name) {
                        conflict = true;
                    }
                    
                    let clean_path = dir_path.join(&clean_name);
                    if clean_path.exists() && clean_path != file_path {
                        conflict = true;
                    }
                    
                    if conflict {
                        let checksum = self.get_checksum_from_database(&file_path)
                            .unwrap_or_else(|_| {
                                let mut hash = String::new();
                                if let Ok(h) = self.calculate_hash(&file_path) {
                                    hash = h;
                                }
                                hash
                            });
                        
                        let hashed_name = self.create_hashed_filename(&clean_name, &checksum);
                        
                        writeln!(file, "# Rename with hash due to conflict: {} -> {}", filename, hashed_name)?;
                        writeln!(file, "cp \"{}\" \"$BACKUP_DIR/{}/{}\"", file_path.display(), display_name, filename)?;
                        writeln!(file, "mv \"{}\" \"{}/{}\"", file_path.display(), dir_path.display(), hashed_name)?;
                    } else {
                        writeln!(file, "# Rename to remove suffix: {} -> {}", filename, clean_name)?;
                        writeln!(file, "cp \"{}\" \"$BACKUP_DIR/{}/{}\"", file_path.display(), display_name, filename)?;
                        writeln!(file, "mv \"{}\" \"{}/{}\"", file_path.display(), dir_path.display(), clean_name)?;
                    }
                    
                    writeln!(file, "")?;
                }
            }
        }
        
        Ok(())
    }
    
    fn display_results(&self) {
        println!("");
        println!("=== Deduplication Analysis Complete ===");
        println!("Total files processed: {}", self.total_files);
        println!("Unique files found: {}", self.unique_files);
        println!("Within-directory duplicates: {}", self.same_dir_dupes);
        println!("Cross-directory duplicates: {}", self.cross_dir_dupes_count);
        println!("Filename cleanup candidates: {}", self.rename_candidates);
        println!("");
        
        // Display performance metrics
        let total_time = self.start_time.elapsed();
        let bytes_processed = ByteSize(self.total_bytes_processed);
        let throughput = if self.hashing_time.as_secs() > 0 {
            ByteSize((self.total_bytes_processed as f64 / self.hashing_time.as_secs_f64()) as u64)
        } else {
            ByteSize(0)
        };
        let memory_usage = ByteSize(self.peak_memory_usage);
        
        println!("=== Performance Metrics ===");
        println!("Total runtime: {}", format_duration(total_time));
        println!("Hashing time: {}", format_duration(self.hashing_time));
        println!("Data processed: {}", bytes_processed);
        println!("Throughput: {}/s", throughput);
        println!("Peak memory usage: {}", memory_usage);
        println!("");
        
        println!("All checksums have been saved to: {}", self.checksum_db_path.display());
        println!("");
        println!("IMPORTANT: Potentially destructive operations have been written to:");
        println!("{}", self.destructive_script_path.display());
        println!("");
        println!("Please review this script carefully before running it!");
        println!("It will:");
        println!("1. Backup files before removing duplicates");
        println!("2. Remove within-directory duplicates (keeping one copy)");
        println!("3. List cross-directory duplicates (commented out, must be manually enabled)");
        println!("4. Clean up filenames by removing numeric suffixes");
        println!("");
        println!("To apply these changes, run: bash {}", self.destructive_script_path.display());
    }
    
    fn get_checksum_from_database(&self, file_path: &Path) -> Result<String> {
        let file = File::open(&self.checksum_db_path)?;
        let reader = BufReader::new(file);
        let path_str = file_path.to_string_lossy();
        
        for line in reader.lines() {
            let line = line?;
            if line.contains(&format!("  {}", path_str)) {
                let parts: Vec<&str> = line.splitn(2, "  ").collect();
                if parts.len() == 2 {
                    return Ok(parts[0].to_string());
                }
            }
        }
        
        Err(anyhow!("Checksum not found for file: {}", path_str))
    }
    
    fn add_to_database(&self, file_path: &Path, checksum: &str) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.checksum_db_path)?;
        
        writeln!(file, "{}  {}", checksum, file_path.to_string_lossy())?;
        
        Ok(())
    }
    
    fn calculate_hash(&mut self, file_path: &Path) -> Result<String> {
        // Track hash calculation time
        let hash_start = Instant::now();
        
        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();
        let bytes_read = file.read_to_end(&mut buffer)?;
        
        // Add to total bytes processed
        self.total_bytes_processed += bytes_read as u64;
        
        // Use XXH3 hash64 which is extremely fast
        let hash_value = hash64(&buffer);
        
        // Track hashing time
        let elapsed = hash_start.elapsed();
        self.hashing_time += elapsed;
        
        // Update memory usage
        self.system_info.refresh_all();
        let pid = std::process::id() as usize;
        if let Some(process) = self.system_info.process(sysinfo::Pid::from(pid)) {
            let memory = process.memory();
            if memory > self.peak_memory_usage {
                self.peak_memory_usage = memory;
            }
        }
        
        // Convert to hex string format
        Ok(format!("{:016x}", hash_value))
    }
    
    fn get_relative_path(&self, path: &Path) -> String {
        path.strip_prefix(&self.root_path)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "".to_string())
    }
    
    fn get_dir_path(&self, filepath: &str) -> String {
        let path = Path::new(filepath);
        let rel_path = path.strip_prefix(&self.root_path)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| filepath.to_string());
            
        Path::new(&rel_path)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "".to_string())
    }
    
    fn has_numeric_suffix(&self, filename: &str) -> bool {
        let basename = filename.rfind('.').map_or(filename, |i| &filename[..i]);
        
        // Create regex patterns for common numeric suffixes
        let re1 = Regex::new(r"-\d+$").unwrap();
        let re2 = Regex::new(r"_\d+$").unwrap();
        let re3 = Regex::new(r"\d{2}$").unwrap();
        
        re1.is_match(basename) || re2.is_match(basename) || re3.is_match(basename)
    }
    
    fn remove_numeric_suffix(&self, filename: &str) -> String {
        let dot_pos = filename.rfind('.');
        let (basename, extension) = match dot_pos {
            Some(pos) => (&filename[..pos], &filename[pos..]),
            None => (filename, ""),
        };
        
        // Remove common numeric suffix patterns
        let re1 = Regex::new(r"-\d+$").unwrap();
        let re2 = Regex::new(r"_\d+$").unwrap();
        let re3 = Regex::new(r"\d{2}$").unwrap();
        
        let temp1 = re1.replace(basename, "");
        let temp2 = re2.replace(&temp1, "");
        let clean_basename = re3.replace(&temp2, "");
        
        format!("{}{}", clean_basename, extension)
    }
    
    fn create_hashed_filename(&self, filename: &str, checksum: &str) -> String {
        let dot_pos = filename.rfind('.');
        let (basename, extension) = match dot_pos {
            Some(pos) => (&filename[..pos], &filename[pos..]),
            None => (filename, ""),
        };
        
        let short_hash = &checksum[..8];
        format!("{}_{}{}", basename, short_hash, extension)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut deduplicator = MediaDeduplicator::new()?;
    deduplicator.run(args)?;
    Ok(())
}
