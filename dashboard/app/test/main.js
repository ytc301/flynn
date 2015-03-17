var config = require('./config').config;

function waitForUrl(matcher, callback, timeout) {
	return casper.waitForUrl(matcher, callback, callback, timeout || 200);
}

function waitForSelector(selector, callback, timeout) {
	return casper.waitForSelector(selector, callback, callback, timeout || 200);
}

function waitWhileSelector(selector, callback, timeout) {
	return casper.waitWhileSelector(selector, callback, callback, timeout || 200);
}

var values = {
	maxAppNameLength: 30
};

var steps = {
	Login: function (test) {
		test.assert(!!casper.page.url.match(/\/login/), 'Login page loaded');
		casper.fillXPath('form', {
			'//input[@type="password"]': config.loginToken
		}, true);
		return waitForUrl(/(^\/login)/, function () {
			test.assert(!casper.page.url.match(/\/login/), 'Login successful');
		});
	},

	GithubAuth: function (test) {
		var githubBtnSelector = '.btn-green[href="/github"]';
		test.assertExists(githubBtnSelector, 'Github button exists');
		test.assertEqual(casper.fetchText(githubBtnSelector), 'Connect with Github', 'Github button reads "Connect with Github"');
		casper.click(githubBtnSelector);
		return waitForUrl(/\/github\/auth/, function () {
			var generateTokenBtnSelector = 'a[href^="https://github.com/settings/tokens/new"]';
			test.assertExists(generateTokenBtnSelector, 'Generate token button exists');
		}).then(function () {
			casper.fillXPath('form', {
				'//input[@type="text"]': config.githubToken
			}, true);
			return waitForUrl(/\/github$/, function () {
				test.assert(!!casper.page.url.match(/\/github$/), 'Github auth successful');
			}, 20000);
		});
	},

	NavigateGithubStarred: function (test) {
		var starredLinkSelector = 'a[href$="github?type=star"]';
		test.assertExists(starredLinkSelector, 'Starred link exists');
		casper.click(starredLinkSelector);
	},

	LaunchExampleRepo: function (test) {
		var exampleRepoLinkSelector = 'a[href*="flynn-examples"]';
		return waitForSelector(exampleRepoLinkSelector, function () {
			test.assertExists(exampleRepoLinkSelector, 'Example repo link exists');
			casper.click(exampleRepoLinkSelector);
			var launchCommitBtnSelector = '.launch-btn';
			return waitForSelector(launchCommitBtnSelector, function () {
				test.assertExists(launchCommitBtnSelector, 'Launch button exists');
			}).then(function () {
				casper.click(launchCommitBtnSelector);
				var launchBtnSelector = '#secondary .launch-btn';
				return waitForSelector(launchBtnSelector, function () {
					var nameInputSelector = '#secondary .name+input[type=text]';
					var postgresCheckboxSelector = '#secondary .name+input[type=checkbox]';
					var newEnvKeyInputSelector = '#secondary .edit-env input';
					var newEnvValueInputSelector = '#secondary .edit-env input+span+input';
					test.assertExists(launchBtnSelector, 'Launch button exists (modal)');
					test.assertExists(nameInputSelector, 'Name input exists');
					test.assertExists(postgresCheckboxSelector, 'Postgres checkbox exists');
					test.assertExists(newEnvKeyInputSelector, 'Env key input exists');
					test.assertExists(newEnvValueInputSelector, 'Env value input exists');
					var fillValues = {};
					fillValues[nameInputSelector] = values.exampleAppName = ('example-app-'+ Date.now()).substr(0, values.maxAppNameLength);
					casper.fillSelectors('body', fillValues);
					values.testEnvKey = ('TEST_'+ Date.now());
					casper.sendKeys(newEnvKeyInputSelector, values.testEnvKey);
					values.testEnvValue = Date.now();
					casper.sendKeys(newEnvValueInputSelector, values.testEnvValue);
					casper.click(postgresCheckboxSelector);
					test.assertExists(postgresCheckboxSelector +':checked', 'Postgres checkbox checked');
					casper.click(launchBtnSelector);
					return waitWhileSelector(launchBtnSelector+ '[disabled]', function () {
						test.assert(casper.fetchText(launchBtnSelector) === 'Continue', 'Example app launched');
					}, 60000 * 5);
				}, 10000);
			});
		}, 10000);
	},

	RemoveGithubToken: function (test) {
		test.assert(!!casper.page.url.match(/\/apps\/dashboard\/env$/), 'Dashboard edit env page loaded');
		return waitForSelector('#secondary input[type=text]', function () {
			test.assertExists('#secondary input[value=GITHUB_TOKEN]', 'GITHUB_TOKEN env is set');
			casper.fillSelectors('#secondary', {
				'input[value=GITHUB_TOKEN]': ''
			});
			casper.click('#secondary .edit-env+button');
			waitForUrl(/\/apps\/dashboard$/, function () {
				test.assert(!!casper.page.url.match(/\/apps\/dashboard$/), 'Env saved');
			}, 10000);
		});
	}
};

casper.test.begin('Dashboard integration test', function (test) {
	casper.start(config.url);

	casper.then(function () {
		return steps.Login(test);
	});

	casper.then(function () {
		return steps.GithubAuth(test);
	});

	casper.then(function () {
		return steps.NavigateGithubStarred(test);
	}).then(function () {
		return steps.LaunchExampleRepo(test);
	});

	casper.thenOpen(config.url +'/apps/dashboard/env').then(function () {
		return steps.RemoveGithubToken(test);
	});

	casper.run(function () {
		test.done();
	});
});
