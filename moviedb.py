import pickle
import shutil
import pandas as pd
import os
import matplotlib.pyplot as plt
from collections import Counter


class MovieDBError(ValueError):
    pass


class MovieDB:
    def __init__(self, data_dir):
        # Initialize directories
        self.data_dir = data_dir
        self.movies_dir = os.path.join(data_dir, 'movies.csv')
        self.directors_dir = os.path.join(data_dir, 'directors.csv')
        self.init_dirs()

    # Helper Functions
    def init_dirs(self):
        try:
            self.movies = pd.read_csv(self.movies_dir)
            self.read_directors_csv()
        except FileNotFoundError:
            # Create tables and files if non-existent
            self.movies = pd.DataFrame(
                columns=['movie_id', 'title', 'year', 'genre', 'director_id'])
            self.directors = pd.DataFrame(
                columns=['director_id', 'given_name',
                         'last_name', 'director'], dtype=int)
            self.movies.to_csv(self.movies_dir, index=False)
            self.write_directors_csv()

    def write_directors_csv(self):
        self.directors.iloc[:, :-1].to_csv(self.directors_dir, index=False)

    def read_directors_csv(self):
        d_temp = pd.read_csv(self.directors_dir, dtype={'director_id': int})
        d_temp['director'] = d_temp['last_name'] + ', ' + d_temp['given_name']
        self.directors = d_temp

    def add_director(self, director):
        self.read_directors_csv()
        director = ' '.join(director.split()).replace(' ,', ',')
        new_d = {'director': director}
        new_d['last_name'], new_d['given_name'] = director.split(', ', 1)
        if len(self.directors) == 0:
            new_d['director_id'] = 1
            self.directors = self.directors.append(new_d, ignore_index=True)
        else:
            # If director is already in list, return the `director_id`
            d_list = list(self.directors['director'].str.lower().values)
            if director.lower() in d_list:
                return self.directors['director_id'].iloc[
                    d_list.index(director.lower())]
            # Add director if not in list
            else:
                # Set new director_id to last_id + 1
                last_id = self.directors['director_id'].iloc[-1]
                new_d['director_id'] = int(last_id+1)
                self.directors = self.directors.append(new_d,
                                                       ignore_index=True)
        self.write_directors_csv()
        return self.directors['director_id'].iloc[-1]

    def is_dup(self, movie):
        dup_check = (self.movies['title'].str.lower() +
                     self.movies['year'].astype(str) +
                     self.movies['genre'].str.lower() +
                     self.movies['director_id'].astype(str))
        search_term = ''.join(str(v).lower() for v in movie.values())
        return search_term in dup_check.values

    # Main Functions

    def add_movie(self, title, year, genre, director):
        self.init_dirs()
        movie = {'title': title.strip(), 'year': year, 'genre': genre.strip()}
        movie['director_id'] = int(self.add_director(director))
        if len(self.movies) == 0:
            movie['movie_id'] = 1
            self.movies = self.movies.append(movie, ignore_index=True)
        else:
            if self.is_dup(movie):
                raise MovieDBError
            else:
                last_id = self.movies['movie_id'].iloc[-1]
                movie['movie_id'] = last_id + 1
                self.movies = self.movies.append(movie, ignore_index=True)
        self.movies.to_csv(self.movies_dir, index=False)
        return self.movies['movie_id'].iloc[-1]

    def add_movies(self, movie_list):
        i = 0
        movie_ids = []

        def i_message(i):
            return (f"Warning: movie index {i} has invalid or incomplete "
                    "information. Skipping...")
        for movie in movie_list:
            keys = ['director', 'genre', 'title', 'year']
            if sorted(movie.keys()) != keys:
                print(i_message(i))
            elif [type(movie[key]) for key in keys] != [str, str, str, int]:
                print(i_message(i))
            elif ', ' not in movie['director']:
                print(i_message(i))
            else:
                try:
                    movie_ids += [self.add_movie(movie['title'],
                                                 movie['year'],
                                                 movie['genre'],
                                                 movie['director'])]
                except MovieDBError:
                    print(f"Warning: movie {movie['title']} is already in the"
                          " database. Skipping...")
            i += 1
        return movie_ids

    def delete_movie(self, movie_id):
        self.init_dirs()
        if movie_id not in self.movies['movie_id'].values:
            raise MovieDBError
        self.movies = self.movies[self.movies['movie_id'] != movie_id]
        self.movies.to_csv(self.movies_dir, index=False)

    def search_movies(self, title=None, year=None, genre=None,
                      director_id=None):
        self.init_dirs()
        if [title, year, genre, director_id] == [None]*4:
            raise MovieDBError
        else:
            search_table = pd.read_csv(self.movies_dir).\
                apply(lambda x: x.str.lower() if(x.dtype == 'object') else x)
            filters = {'title': None if title is None else title.lower().
                       strip(),
                       'genre': None if genre is None else genre.lower().
                       strip(),
                       'year': year,
                       'director_id': director_id}
            for k, v in filters.items():
                if v is not None:
                    search_table = search_table[search_table[k] == v]
        return search_table['movie_id'].tolist()

    def export_data(self):
        self.init_dirs()
        d_temp = pd.merge(self.movies, self.directors, how='left',
                          on='director_id').\
            rename(columns={'given_name': 'director_given_name',
                            'last_name': 'director_last_name'}).\
            sort_values('movie_id')
        ret_cols = ['title', 'year', 'genre', 'director_last_name',
                    'director_given_name']
        return d_temp[ret_cols]

    def generate_statistics(self, stat):
        self.init_dirs()
        if stat == 'movie':
            return self.movies.groupby('year')['movie_id'].count().to_dict()
        elif stat == 'genre':
            return {genre: self.movies[self.movies['genre'] == genre].
                    groupby('year')['movie_id'].count().to_dict() for genre in
                    self.movies['genre'].unique()}
        elif stat == 'director':
            df_temp = pd.merge(self.movies, self.directors, how='left',
                               on='director_id')
            return {director: df_temp[df_temp['director'] == director].
                    groupby('year')['movie_id'].count().to_dict() for director
                    in df_temp['director'].unique()}
        elif stat == 'all':
            return {s: self.generate_statistics(s) for s in
                    ['movie', 'genre', 'director']}
        else:
            raise MovieDBError

    def plot_statistics(self, stat):
        self.init_dirs()
        if stat == 'movie':
            movie_dict = self.generate_statistics('movie')
            movie_dict = {k: v for k, v in sorted(movie_dict.items(),
                                                  key=lambda item: -item[1])}
            plt.rcParams['figure.figsize'] = [15, 10]
            plt.bar(movie_dict.keys(), movie_dict.values())
            plt.xlabel('year')
            plt.ylabel('movies')
            ax = plt.gca()
            plt.show()
            return ax
        elif stat == 'genre':
            genre_dict = self.generate_statistics('genre')
            plt.rcParams['figure.figsize'] = [15, 10]
            for genre in sorted(genre_dict):
                plt.plot(genre_dict[genre].keys(), genre_dict[genre].values(),
                         'o-', label=genre)
            plt.xlabel('year')
            plt.ylabel('movies')
            plt.legend(ncol=2)
            ax = plt.gca()
            plt.show()
            return ax
        elif stat == 'director':
            director_dict = self.generate_statistics('director')
            temp_df = pd.merge(self.movies, self.directors, how='left',
                               on='director_id')
            top_5 = temp_df.groupby(['director', 'director_id'])['movie_id'].\
                count().reset_index().sort_values(['movie_id', 'director'],
                                                  ascending=[False, True]).\
                head(5)['director'].tolist()            
            plt.rcParams['figure.figsize'] = [15, 10]
            for director in top_5:
                plt.plot(director_dict[director].keys(),
                         director_dict[director].values(),
                         'o-', label=director)
            plt.xlabel('year')
            plt.ylabel('movies')
            plt.legend()
            ax = plt.gca()
            plt.show()
            return ax
        else:
            raise MovieDBError

    def token_freq(self):
        self.init_dirs()
        search_list = ' '.join(self.movies['title'].values).lower().split()
        return dict(Counter(search_list))
