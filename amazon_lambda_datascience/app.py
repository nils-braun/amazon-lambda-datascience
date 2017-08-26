def add_routes(app):
    """Add all routes to the application"""
    @app.route('/', methods=['GET'])
    def main():
        """
        Main method: calculate the features of the given csv dataframe.
        """
        return "Nothing to see here!"
