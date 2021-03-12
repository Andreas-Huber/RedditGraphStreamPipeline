#!/bin/bash

time python3 export_user_count.py
time jupyter nbconvert --ExecutePreprocessor.timeout=600 --to notebook --inplace --execute number_users_per_subreddit.ipynb
