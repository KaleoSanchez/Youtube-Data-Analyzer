import os
import sys

def main():
    print("YouTube Data Analyzer")
    print("====================")
    
    data_directory = "youtube_data"
    
    if not os.path.exists(data_directory):
        print(f"Error: Directory '{data_directory}' not found")
        data_directory = input("Please enter the path to the youtube_data directory: ")
        if not os.path.exists(data_directory):
            print(f"Error: Directory '{data_directory}' not found")
            return
    
    try:
        from youtube_analyzer_complete import YouTubeDataAnalyzer
    except ImportError:
        print("Error: Could not import YouTubeDataAnalyzer class.")
        print("Make sure youtube_analyzer_complete.py is in the same directory.")
        return
    
    analyzer = YouTubeDataAnalyzer(data_directory)
    
    analyzer.load_data(
        video_folder="0222",   
        size_folder="0523",     
        user_folder="0528"      
    )
    
    analyzer.run_interactive_mode()

if __name__ == "__main__":
    main()