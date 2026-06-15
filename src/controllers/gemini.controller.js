const { GoogleGenAI } = require('@google/genai');
const axios = require('axios');

exports.generateText = async (req, res) => {
  const textContent = req.body.text;

  try {
    const ai = new GoogleGenAI({
      apiKey: process.env.GEMINI_API_KEY,
    });

    const response = await ai.models.generateContent({
      model: 'gemini-3-flash-preview',
      contents: textContent,
    });

    return res.status(200).json({
      status: 200,
      result: response.text,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      status: 500,
      message: err.message,
    });
  }
};

exports.APIGenerateText = async (req, res) => {
  console.log('Memulai text ai..');
  const textContent = req.body.text;
  try {
    const response = await fetch('https://anabot.my.id/api/ai/chatgpt3', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'gpt-4.1',
        models: 'gpt-4.1',
        apikey: 'freeApikey',
        messages: textContent,
      }),
    });
    const data = await response.json();
    return res.status(200).json({
      status: 200,
      result: data,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      status: 500,
      message: err.message,
    });
  }
};
