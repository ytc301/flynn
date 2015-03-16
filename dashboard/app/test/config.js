var env = require('os').env;
exports.config = {
	dashboardURL: 'http://dashboard.'+ env.CONTROLLER_DOMAIN
};
