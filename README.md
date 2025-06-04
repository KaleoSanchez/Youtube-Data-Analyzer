# Youtube-Data-Analyzer
YOUTUBE DATA ANALYZER
=====================

OVERVIEW
--------
This tool analyzes YouTube data to extract insights about videos, users, and recommendation patterns.

INSTALLATION
-----------
1. Make sure Python 3.6+ is installed on your system
2. Install required packages (run the following in command prompt in this folder):
   pip install -r requirements.txt

DATA STRUCTURE
-------------
Place your YouTube data in a directory structure as follows:

youtube_data/
├── 0222/         - Video data folder
│   ├── 0.txt
│   ├── 1.txt
│   └── ...
├── 0523/         - Size data folder
│   └── size.txt
└── 0528/         - User data folder
    └── user.txt

USAGE
-----
Run the analyzer using:

python run_analyzer_fixed.py

Follow the interactive menu prompts to perform different analyses:
1. Top K Categories
2. Top K Rated Videos
3. Top K Popular Videos
4. Find Videos by Category and Duration
5. Find Videos by Size Range
6. Find Recommendation Patterns
0. Exit

FEATURES
--------
- Data loading from text files
- Finding top video categories
- Identifying highest-rated and most viewed videos
- Filtering videos by category, duration, and size
- Analyzing recommendation patterns
- Visualizing recommendation networks

DATA FORMAT
----------
1. Video Data (0222/*.txt):
   video_id uploader age category length views rate ratings comments related_ids

2. Size Data (0523/size.txt):
   video_id size_in_bytes

3. User Data (0528/user.txt):
   username uploads friends
