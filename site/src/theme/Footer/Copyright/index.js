import React from 'react';
export default function FooterCopyright({ copyright }) {
  return (
    <div
      className="footer__copyright cursor-pointer hover:text-primary-500"
      onClick={() => window.open('https://beian.miit.gov.cn/')}
      // Developer provided the HTML, so assume it's safe.
      // eslint-disable-next-line react/no-danger
      dangerouslySetInnerHTML={{ __html: copyright }}
    />
  );
}
