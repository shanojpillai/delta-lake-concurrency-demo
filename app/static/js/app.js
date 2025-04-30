// Common JavaScript functions for Delta Lake Demo

// Function to show loading spinner
function showLoading() {
    $('.loading').show();
}

// Function to hide loading spinner
function hideLoading() {
    $('.loading').hide();
}

// Function to show alert
function showAlert(message, type = 'success') {
    const alertHtml = `
        <div class="alert alert-${type} alert-dismissible fade show" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        </div>
    `;
    $('#alerts-container').html(alertHtml);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        $('.alert').alert('close');
    }, 5000);
}

// Function to fetch table info
function fetchTableInfo() {
    $.ajax({
        url: '/api/table_info',
        method: 'GET',
        success: function(response) {
            if (response.success) {
                updateTableInfo(response.metrics, response.history);
            } else {
                showAlert('Failed to fetch table info: ' + response.error, 'danger');
            }
        },
        error: function(xhr, status, error) {
            showAlert('Error fetching table info: ' + error, 'danger');
        }
    });
}

// Function to update table info display
function updateTableInfo(metrics, history) {
    if (metrics) {
        $('#table-version').text(metrics.current_version);
        $('#record-count').text(metrics.record_count.toLocaleString());
        $('#file-count').text(metrics.num_files.toLocaleString());
        $('#table-size').text(metrics.size_in_mb.toFixed(2) + ' MB');
    }
    
    if (history && history.length > 0) {
        const historyHtml = history.map(entry => `
            <tr>
                <td>${entry.version}</td>
                <td>${new Date(entry.timestamp).toLocaleString()}</td>
                <td>${entry.operation}</td>
            </tr>
        `).join('');
        
        $('#history-table tbody').html(historyHtml);
    }
}

// Document ready
$(document).ready(function() {
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Fetch table info on page load if the table info section exists
    if ($('#table-info').length > 0) {
        fetchTableInfo();
    }
});
