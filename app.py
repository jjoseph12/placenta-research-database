"""
Flask application for GEO Metadata website.
Provides a searchable interface for browsing GEO experiment metadata.
"""

from flask import Flask, render_template, request, jsonify
from database import search_entries, get_all_entries, get_entry_by_id, get_column_info, get_filter_options, search_with_filters

app = Flask(__name__)

@app.route('/')
def index():
    """Homepage with search interface."""
    query = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int)
    
    # Get filter values from request
    selected_organisms = request.args.getlist('organism')
    selected_data_types = request.args.getlist('data_type')
    selected_strategies = request.args.getlist('library_strategy')
    
    # Build filters dict
    filters = {}
    if selected_organisms:
        filters['organisms'] = selected_organisms
    if selected_data_types:
        filters['data_types'] = selected_data_types
    if selected_strategies:
        filters['library_strategies'] = selected_strategies
    
    data = search_with_filters(query=query, filters=filters if filters else None, page=page)
    
    # Get filter options for the checkbox UI
    filter_options = get_filter_options()
    
    return render_template('index.html', 
                         data=data, 
                         query=query,
                         column_info=get_column_info(),
                         filter_options=filter_options,
                         selected_organisms=selected_organisms,
                         selected_data_types=selected_data_types,
                         selected_strategies=selected_strategies)

@app.route('/entry/<int:object_id>')
def entry_detail(object_id):
    """Detail page for a single entry."""
    entry = get_entry_by_id(object_id)
    if not entry:
        return render_template('404.html'), 404
    
    return render_template('entry.html', 
                         entry=entry, 
                         column_info=get_column_info())

@app.route('/api/search')
def api_search():
    """JSON API for search (for AJAX requests)."""
    query = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int)
    
    if query:
        data = search_entries(query, page=page)
    else:
        data = get_all_entries(page=page)
    
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
