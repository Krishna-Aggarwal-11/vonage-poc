import { unlink } from 'node:fs/promises';
import { basename, extname } from 'node:path';
import { v2 as cloudinary } from 'cloudinary';

let isConfigured = false;

function configureCloudinary() {
  if (isConfigured) return;
  if (!process.env.CLOUDINARY_URL) {
    const { CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET } = process.env;
    if (!CLOUDINARY_CLOUD_NAME || !CLOUDINARY_API_KEY || !CLOUDINARY_API_SECRET) {
      throw new Error('Set CLOUDINARY_URL or CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, and CLOUDINARY_API_SECRET');
    }
    cloudinary.config({
      cloud_name: CLOUDINARY_CLOUD_NAME,
      api_key: CLOUDINARY_API_KEY,
      api_secret: CLOUDINARY_API_SECRET,
      secure: true
    });
  }
  isConfigured = true;
}

export async function uploadAudioFile(path) {
  configureCloudinary();
  const publicId = basename(path, extname(path));
  const result = await cloudinary.uploader.upload(path, {
    resource_type: 'video',
    folder: 'vonage-recordings',
    public_id: publicId,
    overwrite: false
  });

  // Cloudinary now holds the durable copy; remove the temporary Vercel file.
  await unlink(path).catch(() => {});
  return {
    publicId: result.public_id,
    url: result.secure_url
  };
}
