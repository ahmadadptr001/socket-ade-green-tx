const supabase = require("../configs/supabase.config");
const nodemailer = require("nodemailer");
const bcrypt = require("bcryptjs");

exports.sendNotification = async (req, res) => {
  const message = req.body;

  try {
    const response = await fetch("https://exp.host/--/api/v2/push/send", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(message),
    });

    const result = await response.json();

    // Expo balas error
    if (result?.data?.status === "error") {
      return res.status(400).json({
        status: 400,
        message: result.data.message,
        details: result.data.details,
      });
    }

    // Berhasil
    return res.json({
      status: 200,
      message: "Notifikasi terkirim",
      expoResponse: result,
    });
  } catch (err) {
    console.error("ERROR LOG NOTIFICATION:", err);

    return res.status(500).json({
      status: 500,
      message: err.message,
    });
  }
};

exports.sendOTP = async (req, res) => {
  try {
    const { email } = req.body;

    const now = new Date().toISOString();
    const { data: recentOtp } = await supabase
      .from("otps")
      .select("created_at")
      .eq("email", email)
      .order("created_at", { ascending: false })
      .limit(1)
      .single();
    if (
      recentOtp &&
      new Date(now) - new Date(recentOtp.created_at) < 60 * 1000
    ) {
      return res.status(429).json({
        status: 429,
        message: "Tunggu sebentar sebelum meminta OTP lagi.",
      });
    }

    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = new Date(Date.now() + 5 * 60 * 1000); // 5 menit

    const { data, error } = await supabase
      .from("otps")
      .insert([{ email, code: otp, expires_at: expiresAt }]);

    if (error) {
      res.json({ status: 500, message: error.message });
    }

    const transporter = nodemailer.createTransport({
      host: "smtp.hostinger.com",
      port: 465,
      secure: true,
      auth: {
        user: process.env.SMTP_EMAIL,
        pass: process.env.SMTP_PASSWORD,
      },
    });

    await transporter.sendMail({
      from: '"Ade Green TX" support@adegreentx.id',
      to: email,
      subject: "Kode OTP Anda",
      html: `
      <div style="max-width: 700px; margin: auto; font-family: 'Segoe UI', sans-serif; background-color: #ffffff; padding: 40px; border-radius: 16px; box-shadow: 0 10px 40px rgba(0,0,0,0.1);">
        <div style="text-align: center;">
          <img src="https://adegreentx.id/text-2.png" alt="ADE Green TX" style="height: 60px; margin-bottom: 30px;" />
          <p style="font-size: 18px; color: #555;">Gunakan kode di bawah ini untuk melanjutkan proses verifikasi Anda.</p>
        </div>

        <div style="margin: 40px 0; text-align: center;">
          <div style="display: inline-block; padding: 20px 40px; font-size: 48px; font-weight: bold; background-color: #f0f4ff; color: #2f54eb; border-radius: 12px; letter-spacing: 8px;">
            ${otp}
          </div>
        </div>

        <p style="font-size: 16px; color: #666; text-align: center;">
          Kode ini hanya berlaku selama <strong>5 menit</strong>. Jangan bagikan kepada siapa pun.
        </p>

        <hr style="margin: 40px 0; border: none; border-top: 1px solid #eee;" />

        <p style="font-size: 14px; color: #999; text-align: center;">
          Email ini dikirim secara otomatis oleh sistem <strong>ADE Green TX</strong>. Jika Anda tidak meminta kode ini, abaikan saja email ini.
        </p>
      </div>
    `,
    });

    res.json({
      status: 200,
      message: "Kode otp berhasil terkirim ke " + email,
    });
  } catch (e) {
    res.json({
      status: 429,
      message: e.message,
    });
  }
};

exports.getHalo = (req, res) => res.json({ message: "halo" });

exports.hashPassword = async (req, res) => {
  try {
    const { password } = req.body;
    const salt = bcrypt.genSaltSync(10);
    const pwHash = bcrypt.hashSync(password, salt);
    res.json({
      status: 200,
      meessage: "Berhasil melakukan hash password",
      hashPassword: pwHash,
    });
  } catch (e) {
    res.json({
      status: 429,
      message: e.message,
    });
  }
};
