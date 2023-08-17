import React from 'react';

const IconClipper = ({ imageUrl, icons, targetIcon, scaledWidth, scaledHeight, originScale }) => {
  const iconToRender = icons.find((icon) => icon.id === targetIcon);

  if (!iconToRender) {
    return null;
  }

  const { x, y, width, height } = iconToRender;

  const scaleWidth = scaledWidth / width;
  const scaleHeight = scaledHeight / height;
  const scale = Math.min(scaleWidth, scaleHeight);

  const scaledIconWidth = width * scale;
  const scaledIconHeight = height * scale;
  const scaledIconX = x * scale;
  const scaledIconY = y * scale;

  return (
    <div style={{ width: `${scaledWidth}px`, height: `${scaledHeight}px` }}>
      <div
        style={{
          width: `${scaledIconWidth}px`,
          height: `${scaledIconHeight}px`,
          backgroundImage: `url(${imageUrl})`,
          backgroundPosition: `${-scaledIconX}px ${-scaledIconY}px`,
          backgroundSize: `${scaledWidth * (iconToRender.originScale ?? originScale)}px ${
            scaledHeight * (iconToRender.originScale ?? originScale)
          }px`,
        }}
      ></div>
    </div>
  );
};

export default IconClipper;
