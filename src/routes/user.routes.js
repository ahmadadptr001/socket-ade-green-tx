const express = require('express');
const router = express.Router();
const userController = require('../controllers/user.controller');
const geminiController = require('../controllers/gemini.controller');

router.post('/otp/send', userController.sendOTP);
router.post('/notification/send', userController.sendNotification);
router.post('/password/hash', userController.hashPassword);
router.post('/gemini/generate/text', geminiController.generateText);
router.post('/ai/generate/text', geminiController.APIGenerateText);
router.get('/', userController.getHalo);

module.exports = router;
