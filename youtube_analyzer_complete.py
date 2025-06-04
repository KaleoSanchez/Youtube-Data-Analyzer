import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx
from typing import List, Dict, Set, Tuple, Optional
from collections import defaultdict

class YouTubeDataAnalyzer:
    
    def __init__(self, data_directory: str):
        self.data_directory = data_directory
        
        self.video_df = None
        self.size_df = None
        self.user_df = None
        self.related_df = None
        
    def load_data(self, video_folder: str, size_folder: str = None, user_folder: str = None):
        print("Loading data from folders...")
        
        if video_folder:
            video_path = os.path.join(self.data_directory, video_folder)
            if os.path.exists(video_path):
                self.video_df = self._extract_video_data(video_path)
                if self.video_df is not None:
                    self.related_df = self._extract_related_videos()
            else:
                print(f"Error: Video folder {video_path} not found")
                
        if size_folder:
            size_path = os.path.join(self.data_directory, size_folder)
            if os.path.exists(size_path):
                self.size_df = self._extract_size_data(size_path)
            else:
                print(f"Error: Size folder {size_path} not found")
                
        if user_folder:
            user_path = os.path.join(self.data_directory, user_folder)
            if os.path.exists(user_path):
                self.user_df = self._extract_user_data(user_path)
            else:
                print(f"Error: User folder {user_path} not found")
        
        print("Data loading completed.")
    
    def _extract_video_data(self, folder_path: str):
        print(f"Extracting video data from {folder_path}...")
        
        try:
            data_files = [f for f in os.listdir(folder_path) if f.endswith('.txt') and f[0].isdigit()]
        except Exception as e:
            print(f"Error accessing directory {folder_path}: {e}")
            return None
        
        if not data_files:
            print(f"No data files found in {folder_path}")
            return None
        
        all_data = []
        for data_file in sorted(data_files):
            file_path = os.path.join(folder_path, data_file)
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        line = line.strip()
                        parts = line.split('\t')
                        
                        if len(parts) >= 10:
                            video_id = parts[0]
                            uploader = parts[1]
                            age = int(parts[2]) if parts[2].isdigit() else 0
                            category = parts[3]
                            length = int(parts[4]) if parts[4].isdigit() else 0
                            views = int(parts[5]) if parts[5].isdigit() else 0
                            rate = float(parts[6]) if parts[6].replace('.', '', 1).isdigit() else 0.0
                            ratings = int(parts[7]) if parts[7].isdigit() else 0
                            comments = int(parts[8]) if parts[8].isdigit() else 0
                            related_ids = parts[9]
                            
                            all_data.append({
                                'video_id': video_id,
                                'uploader': uploader,
                                'age': age,
                                'category': category,
                                'length': length,
                                'views': views,
                                'rate': rate,
                                'ratings': ratings,
                                'comments': comments,
                                'related_ids': related_ids
                            })
                            
                print(f"Processed {file_path} - Added {len(all_data)} records")
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue
        
        if not all_data:
            print("No data could be read from the files")
            return None
        
        try:
            video_df = pd.DataFrame(all_data)
            video_df['related_ids_list'] = video_df['related_ids'].apply(lambda x: x.split(',') if x else [])
            print(f"Successfully loaded {len(video_df)} video records")
            return video_df
        except Exception as e:
            print(f"Error creating DataFrame: {e}")
            return None
    
    def _extract_size_data(self, folder_path: str):
        print(f"Extracting size data from {folder_path}...")
        
        size_file_path = os.path.join(folder_path, "size.txt")
        
        if not os.path.exists(size_file_path):
            print(f"Size data file not found at {size_file_path}")
            try:
                for file in os.listdir(folder_path):
                    if file.endswith('.txt') and ('size' in file.lower()):
                        size_file_path = os.path.join(folder_path, file)
                        print(f"Using alternative size file: {size_file_path}")
                        break
                else:
                    return None
            except Exception as e:
                print(f"Error searching for size files: {e}")
                return None
        
        try:
            size_data = []
            
            with open(size_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    parts = line.split('\t')
                    
                    if len(parts) >= 2:
                        video_id = parts[0]
                        size = int(parts[1]) if parts[1].isdigit() else 0
                        
                        size_data.append({
                            'video_id': video_id,
                            'size': size
                        })
            
            if not size_data:
                print(f"No data in {size_file_path}")
                return None
            
            size_df = pd.DataFrame(size_data)
            print(f"Successfully loaded {len(size_df)} size records")
            
            return size_df
            
        except Exception as e:
            print(f"Error reading size data: {e}")
            return None
    
    def _extract_user_data(self, folder_path: str):
        print(f"Extracting user data from {folder_path}...")
        
        user_file_path = os.path.join(folder_path, "user.txt")
        
        if not os.path.exists(user_file_path):
            print(f"User data file not found at {user_file_path}")
            try:
                for file in os.listdir(folder_path):
                    if file.endswith('.txt') and ('user' in file.lower()) and ('id' not in file.lower()):
                        user_file_path = os.path.join(folder_path, file)
                        print(f"Using alternative user file: {user_file_path}")
                        break
                else:
                    return None
            except Exception as e:
                print(f"Error searching for user files: {e}")
                return None
        
        try:
            user_data = []
            
            with open(user_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    parts = line.split('\t')
                    
                    if len(parts) >= 3:
                        username = parts[0]
                        uploads = int(parts[1]) if parts[1].isdigit() else 0
                        friends = parts[2]
                        
                        user_data.append({
                            'username': username,
                            'uploads': uploads,
                            'friends': friends
                        })
            
            if not user_data:
                print(f"No data in {user_file_path}")
                return None
            
            user_df = pd.DataFrame(user_data)
            user_df['friends_list'] = user_df['friends'].apply(lambda x: x.split(',') if x else [])
            print(f"Successfully loaded {len(user_df)} user records")
            
            return user_df
            
        except Exception as e:
            print(f"Error reading user data: {e}")
            return None
    
    def _extract_related_videos(self):
        if self.video_df is None:
            return None
        
        print("Creating related videos mapping...")
        
        relationships = []
        
        for _, row in self.video_df.iterrows():
            source_id = row['video_id']
            related_ids = row['related_ids_list']
            
            for target_id in related_ids:
                if target_id:
                    relationships.append({
                        'source_id': source_id,
                        'target_id': target_id
                    })
        
        related_df = pd.DataFrame(relationships)
        print(f"Created {len(related_df)} video relationship records")
        
        return related_df
    
    def get_top_k_categories(self, k: int = 10):
        if self.video_df is None:
            print("Error: Video data not loaded")
            return None
        
        print(f"Finding top {k} categories with the most videos...")
        
        category_counts = self.video_df['category'].value_counts().reset_index()
        category_counts.columns = ['category', 'count']
        
        top_categories = category_counts.head(k)
        
        return top_categories
    
    def get_top_k_rated_videos(self, k: int = 10, min_ratings: int = 10):
        if self.video_df is None:
            print("Error: Video data not loaded")
            return None
        
        print(f"Finding top {k} rated videos...")
        
        filtered_df = self.video_df[self.video_df['ratings'] >= min_ratings]
        top_rated = filtered_df[['video_id', 'uploader', 'category', 'rate', 'ratings', 'views']]
        top_rated = top_rated.sort_values(by=['rate', 'ratings'], ascending=False).head(k)
        
        return top_rated
    
    def get_top_k_popular_videos(self, k: int = 10):
        if self.video_df is None:
            print("Error: Video data not loaded")
            return None
        
        print(f"Finding top {k} most popular videos...")
        
        top_popular = self.video_df[['video_id', 'uploader', 'category', 'views', 'rate', 'comments']]
        top_popular = top_popular.sort_values(by='views', ascending=False).head(k)
        
        return top_popular
    
    def find_videos_by_category_and_duration(self, category: str, min_duration: int, max_duration: int):
        if self.video_df is None:
            print("Error: Video data not loaded")
            return None
        
        print(f"Finding videos in category '{category}' with duration between {min_duration} and {max_duration} seconds...")
        
        result = self.video_df[
            (self.video_df['category'] == category) & 
            (self.video_df['length'] >= min_duration) & 
            (self.video_df['length'] <= max_duration)
        ][['video_id', 'uploader', 'category', 'length', 'views', 'rate']]
        
        return result
    
    def find_videos_by_size_range(self, min_size: int, max_size: int):
        """Find all videos with size in range [x,y]."""
        if self.video_df is None:
            print("Error: Video data not loaded")
            return None
        
        print(f"Finding videos with size between {min_size} and {max_size} bytes...")
        
        size_file_path = os.path.join(self.data_directory, "0523", "size.txt")
        
        try:
            size_data = []
            with open(size_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        video_id = parts[0]
                        size = int(parts[1]) if parts[1].isdigit() else 0
                        size_data.append({'video_id': video_id, 'size': size})
            
            size_df = pd.DataFrame(size_data)
            
            video_with_size = pd.merge(self.video_df, size_df, on='video_id')
            
            result = video_with_size[
                (video_with_size['size'] >= min_size) & 
                (video_with_size['size'] <= max_size)
            ][['video_id', 'uploader', 'category', 'size', 'views']]  
            
            result = result.sort_values(by=['size', 'views'], ascending=[False, False])
            
            return result
        
        except Exception as e:
            print(f"Error reading size data: {e}")
            return None
    
    def find_recommendation_patterns(self, pattern_type: str, min_connections: int = 3):
        print(f"Finding {pattern_type} recommendation patterns...")
        
        G = nx.DiGraph()
        pattern_data = []
        
        if pattern_type == "user_video_user":
            if self.video_df is None:
                print("Error: Video data must be loaded for user_video_user pattern")
                return None, None
            
            print("Looking for user connections through video relationships...")
            
            video_to_uploader = {}
            for _, row in self.video_df.iterrows():
                if pd.notna(row['uploader']) and row['uploader'] != '':
                    video_to_uploader[row['video_id']] = row['uploader']
            
            user_connections = defaultdict(int)
            
            processed_count = 0
            total_videos = len(self.video_df)
            
            for _, row in self.video_df.iterrows():
                processed_count += 1
                if processed_count % 1000 == 0:
                    print(f"Processed {processed_count}/{total_videos} videos...")
                
                source_video = row['video_id']
                source_uploader = row['uploader']
                
                if pd.isna(source_uploader) or source_uploader == '':
                    continue
                    
                related_videos = row['related_ids_list'] if 'related_ids_list' in row else []
                if not related_videos and 'related_ids' in row:
                    related_videos = row['related_ids'].split(',') if pd.notna(row['related_ids']) else []
                
                for related_video in related_videos:
                    if related_video in video_to_uploader:
                        related_uploader = video_to_uploader[related_video]
                        if related_uploader == source_uploader:
                            continue
                        user_pair = tuple(sorted([source_uploader, related_uploader]))
                        user_connections[user_pair] += 1
            
            strong_connections = {pair: count for pair, count in user_connections.items() 
                                if count >= min_connections}
            
            print(f"Found {len(strong_connections)} user connections with at least {min_connections} shared videos")
            
            for (user1, user2), count in strong_connections.items():
                pattern_data.append({
                    'user1': user1,
                    'user2': user2,
                    'count': count
                })
                
                G.add_edge(user1, user2, weight=count)
            
            if pattern_data:
                pattern_df = pd.DataFrame(pattern_data)
                pattern_df = pattern_df.sort_values(by='count', ascending=False)
            else:
                pattern_df = pd.DataFrame(columns=['user1', 'user2', 'count'])
                
        elif pattern_type == "video_user_video":
            if self.video_df is None:
                print("Error: Video data must be loaded for video_user_video pattern")
                return None, None
                
            print("Finding videos uploaded by the same user...")
            
            uploader_videos = defaultdict(list)
            
            valid_videos = self.video_df[self.video_df['uploader'].notna() & (self.video_df['uploader'] != '')]
            
            for _, row in valid_videos.iterrows():
                uploader = row['uploader']
                video_id = row['video_id']
                uploader_videos[uploader].append(video_id)
            
            uploaders_with_multiple = sum(1 for uploader, videos in uploader_videos.items() if len(videos) > 1)
            print(f"Found {uploaders_with_multiple} uploaders with multiple videos")
            
            count = 0
            limit = 100  
            
            for uploader, videos in uploader_videos.items():
                if len(videos) > 1:
                    pairs_for_this_uploader = 0
                    
                    for i in range(len(videos)):
                        if pairs_for_this_uploader >= 5:
                            break
                            
                        for j in range(i+1, min(i+6, len(videos))):
                            video1 = videos[i]
                            video2 = videos[j]
                            
                            pattern_data.append({
                                'video1': video1,
                                'video2': video2,
                                'uploader': uploader
                            })
                            
                            G.add_edge(video1, video2, uploader=uploader)
                            
                            pairs_for_this_uploader += 1
                            count += 1
                            
                            if count >= limit:
                                break
                                
                        if count >= limit:
                            break
                if count >= limit:
                    break
            
            print(f"Created {count} video-user-video connections for visualization")
            
            if pattern_data:
                pattern_df = pd.DataFrame(pattern_data)
            else:
                pattern_df = pd.DataFrame(columns=['video1', 'video2', 'uploader'])
        
        elif pattern_type == "triangle":
            if self.video_df is None:
                print("Error: Video data must be loaded for triangle pattern")
                return None, None
                
            print("Looking for triangle patterns in video recommendations...")
            
            video_related = {}
            for _, row in self.video_df.iterrows():
                video_id = row['video_id']
                
                if 'related_ids_list' in row and isinstance(row['related_ids_list'], list):
                    related_ids = row['related_ids_list']
                elif 'related_ids' in row and pd.notna(row['related_ids']):
                    related_ids = row['related_ids'].split(',')
                else:
                    related_ids = []
                    
                related_ids = [rid for rid in related_ids if rid]
                
                if related_ids:
                    video_related[video_id] = related_ids
            
            print(f"Analyzing {len(video_related)} videos with related video information")
            
            triangle_count = 0
            limit = 50 
            
            videos_with_relations = list(video_related.keys())
            
            import random
            random.shuffle(videos_with_relations)
            
            processed_triangles = set()
            
            for video1 in videos_with_relations:
                if triangle_count >= limit:
                    break
                    
                for video2 in video_related.get(video1, []):
                    if video2 not in video_related:
                        continue
                        
                    for video3 in video_related.get(video2, []):
                        if video3 not in video_related:
                            continue
                            
                        if video1 in video_related.get(video3, []):
                            triangle = tuple(sorted([video1, video2, video3]))
                            
                            if triangle not in processed_triangles:
                                processed_triangles.add(triangle)
                                pattern_data.append({
                                    'video1': video1,
                                    'video2': video2,
                                    'video3': video3
                                })
                                
                                G.add_edge(video1, video2)
                                G.add_edge(video2, video3)
                                G.add_edge(video3, video1)
                                
                                triangle_count += 1
                                if triangle_count >= limit:
                                    break
            
            print(f"Found {triangle_count} triangle patterns")
            
            if pattern_data:
                pattern_df = pd.DataFrame(pattern_data)
            else:
                pattern_df = pd.DataFrame(columns=['video1', 'video2', 'video3'])
            
        else:
            print(f"Error: Unknown pattern type '{pattern_type}'")
            return None, None
        
        return G, pattern_df
    
    def visualize_graph(self, G, title: str, filename: str = None):
        if not G or len(G.nodes) == 0:
            print("No graph data to visualize")
            return
            
        plt.figure(figsize=(12, 8))
        
        try:
            if len(G.nodes) < 30:
                pos = nx.spring_layout(G, seed=42)
            elif len(G.nodes) < 100:
                pos = nx.kamada_kawai_layout(G)
            else:
                pos = nx.random_layout(G, seed=42)
                
            if len(G.nodes) > 100:
                subset_nodes = list(G.nodes())[:100]
                G_subset = G.subgraph(subset_nodes)
                nx.draw(G_subset, pos, with_labels=True, node_color='lightblue', 
                        node_size=500, edge_color='gray', arrows=True, 
                        font_size=8, font_weight='bold')
                plt.title(f"{title} (showing first 100 nodes of {len(G.nodes)} total)")
            else:
                nx.draw(G, pos, with_labels=True, node_color='lightblue', 
                        node_size=500, edge_color='gray', arrows=True, 
                        font_size=8, font_weight='bold')
                plt.title(title)
            
            if filename:
                plt.savefig(filename, dpi=300, bbox_inches='tight')
                print(f"Graph saved to {filename}")
            else:
                plt.show()
                
        except Exception as e:
            print(f"Error visualizing graph: {e}")
            try:
                plt.figure(figsize=(10, 6))
                nx.draw(G, with_labels=False, node_size=50, node_color='blue', edge_color='gray', arrows=False)
                plt.title(f"{title} (simplified view)")
                plt.show()
            except Exception as e2:
                print(f"Even simplified visualization failed: {e2}")
    
    def display_results(self, df, title: str, limit: int = None):
        print(f"\n=== {title} ===")
        
        if df is None or len(df) == 0:
            print("No results found.")
            return
        
        if limit is None:
            limit = len(df)
        
        display_df = df.copy().reset_index(drop=True)
        
        display_df.index = range(1, len(display_df) + 1)
        display_df.index.name = '#'
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.colheader_justify', 'center')
        
        print(display_df.head(limit).to_string())
        print(f"Total results: {len(df)}")
        
    def run_interactive_mode(self):
        print("\nYouTube Data Analyzer - Interactive Mode")
        print("=======================================")
        
        while True:
            print("\nSelect an option:")
            print("1. Top K Categories")
            print("2. Top K Rated Videos")
            print("3. Top K Popular Videos")
            print("4. Find Videos by Category and Duration")
            print("5. Find Videos by Size Range")
            print("6. Find Recommendation Patterns")
            print("0. Exit")
            
            try:
                choice = input("\nEnter your choice: ")
                
                if choice == "1":
                    try:
                        k = int(input("Enter K (number of top categories): "))
                        result = self.get_top_k_categories(k)
                        self.display_results(result, f"Top {k} Categories")
                    except ValueError:
                        print("Invalid input. Please enter a valid number.")
                    except Exception as e:
                        print(f"Error: {e}")
                    
                elif choice == "2":
                    try:
                        k = int(input("Enter K (number of top rated videos): "))
                        min_ratings = int(input("Enter minimum number of ratings: "))
                        result = self.get_top_k_rated_videos(k, min_ratings)
                        self.display_results(result, f"Top {k} Rated Videos")
                    except ValueError:
                        print("Invalid input. Please enter valid numbers.")
                    except Exception as e:
                        print(f"Error: {e}")
                    
                elif choice == "3":
                    try:
                        k = int(input("Enter K (number of top popular videos): "))
                        result = self.get_top_k_popular_videos(k)
                        self.display_results(result, f"Top {k} Popular Videos")
                    except ValueError:
                        print("Invalid input. Please enter a valid number.")
                    except Exception as e:
                        print(f"Error: {e}")
                    
                elif choice == "4":
                    try:
                        category = input("Enter category: ")
                        min_duration = int(input("Enter minimum duration (seconds): "))
                        max_duration = int(input("Enter maximum duration (seconds): "))
                        result = self.find_videos_by_category_and_duration(category, min_duration, max_duration)
                        self.display_results(result, f"Videos in '{category}' with duration [{min_duration}, {max_duration}]")
                    except ValueError:
                        print("Invalid input. Please enter valid numbers for durations.")
                    except Exception as e:
                        print(f"Error: {e}")
                    
                elif choice == "5":
                    try:
                        min_size = int(input("Enter minimum size (bytes): "))
                        max_size = int(input("Enter maximum size (bytes): "))
                        result = self.find_videos_by_size_range(min_size, max_size)
                        self.display_results(result, f"Videos with size [{min_size}, {max_size}]")
                    except ValueError:
                        print("Invalid input. Please enter valid numbers for sizes.")
                    except Exception as e:
                        print(f"Error: {e}")
                    
                elif choice == "6":
                    print("\nPattern Types:")
                    print("1. User-Video-User (users connected through video relationships)")
                    print("2. Video-User-Video (videos uploaded by the same user)")
                    print("3. Triangle (video1 -> video2 -> video3 -> video1)")
                    
                    try:
                        pattern_choice = input("Enter pattern type (1-3): ")
                        
                        if pattern_choice == "1":
                            try:
                                min_connections = int(input("Enter minimum connections: "))
                                G, result = self.find_recommendation_patterns("user_video_user", min_connections)
                                self.display_results(result, "User-Video-User Patterns")
                                if G and len(G.nodes) > 0:
                                    self.visualize_graph(G, "User-Video-User Recommendation Patterns")
                            except ValueError:
                                print("Invalid input. Please enter a valid number for minimum connections.")
                            except Exception as e:
                                print(f"Error in User-Video-User pattern: {e}")
                                
                        elif pattern_choice == "2":
                            try:
                                G, result = self.find_recommendation_patterns("video_user_video")
                                self.display_results(result, "Video-User-Video Patterns")
                                if G and len(G.nodes) > 0:
                                    self.visualize_graph(G, "Video-User-Video Recommendation Patterns")
                            except Exception as e:
                                print(f"Error in Video-User-Video pattern: {e}")
                                
                        elif pattern_choice == "3":
                            try:
                                G, result = self.find_recommendation_patterns("triangle")
                                self.display_results(result, "Triangle Patterns")
                                if G and len(G.nodes) > 0:
                                    self.visualize_graph(G, "Triangle Recommendation Patterns")
                            except Exception as e:
                                print(f"Error in Triangle pattern: {e}")
                                
                        else:
                            print("Invalid pattern type.")
                    except Exception as e:
                        print(f"Error in pattern analysis: {e}")
                    
                elif choice == "0":
                    print("Exiting...")
                    break
                    
                else:
                    print("Invalid choice.")
            except Exception as e:
                print(f"An error occurred: {e}")
                continue


if __name__ == "__main__":
    data_directory = "youtube_data"
    
    analyzer = YouTubeDataAnalyzer(data_directory)
    
    analyzer.load_data(
        video_folder="0222",   
        size_folder="0523",    
        user_folder="0528"      
    )
    
    analyzer.run_interactive_mode()