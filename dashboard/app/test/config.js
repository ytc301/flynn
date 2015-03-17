var env = require('system').env;
exports.config = {
	url: env.URL,
	loginToken: env.LOGIN_TOKEN,
	githubToken: env.GITHUB_TOKEN
};
