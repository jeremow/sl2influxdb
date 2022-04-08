from kivy.lang import Builder
from kivy.core.window import Window
# from kivy.config import Config
# from screeninfo import get_monitors
#
# Window.left = 45
# Window.top = 45
#
# width, height = get_monitors()[0].width, get_monitors()[0].height
# Window.size = (width-90, height-90)

css = """
<Widget>:
    font_size: dp(20)
    font_name: 'css/fonts/TCM_____.TTF'
"""

Builder.load_string(css)
