import React from "react";
import clsx from "clsx";

export default function FooterLayout({ style, links, logo, copyright }) {
  return (
    <footer
      className={clsx("footer", {
        "footer--dark": style === "dark",
      })}
    >
      <div className="container container-fluid max-w-7xl mx-auto">
        {links}
        {(logo || copyright) && (
          <div className="footer__bottom flex flex-row justify-between flex-wrap gap-2 items-center">
            {logo && <div className="margin-bottom--sm">{logo}</div>}
            {copyright}
          </div>
        )}
      </div>
    </footer>
  );
}
