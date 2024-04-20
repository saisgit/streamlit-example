import streamlit as st
st.title("Sectors")

import time

button = st.button("start")
placeholder = st.empty()

if button:
    with st.spinner():
        counter = 0
        while True:
            print("Waiting...")
            if placeholder.button("Stop", key=counter): # otherwise streamlit complains that you're creating two of the same widget
                break
            time.sleep(3)
            counter += 1

st.write("done")  # in this sample this code never executed